package Plugins::YouTube::Download;

# Download handler for the LMS YouTube plugin
# Dispatches yt-dlp downloads for individual videos and playlists,
# triggered via CLI/JSON-RPC or the in-app Download menu item.
#
# CLI syntax (telnet / json-rpc):
#   youtube download url:<url>
#   where <url> is  youtube://tU0_rKD8qjw
#                or ytplaylist://playlistId=PLFgquLn...
#                or ytplaylist://channelId=UC...
#
# Released under GPLv2

use strict;
use warnings;

use File::Spec;
use POSIX qw(O_RDONLY O_WRONLY O_CREAT O_APPEND strftime);

use Slim::Utils::Log;
use Slim::Utils::Prefs;
use Slim::Utils::OSDetect;

use Plugins::YouTube::Utils;

my $log   = logger('plugin.youtube');
my $prefs = preferences('plugin.youtube');

# ─── Public entry points ────────────────────────────────────────────────────

# Called from Plugin::initPlugin to wire up the CLI command.
sub registerCLI {
	#        |requires Client
	#        |  |is a Query
	#        |  |  |has Tags
	#        |  |  |  |Function to call
	Slim::Control::Request::addDispatch(
		['youtube', 'download'],
		[0, 0, 0, \&cliDownload],
	);
	$log->info('YouTube download CLI command registered');
}

# CLI handler:
#   Positional:  ["youtube","download","<url>"]
#   Tagged:      youtube download url:<url>
#
# Accepted URL forms:
#   youtube://www.youtube.com/v/<id>   (internal stream URL)
#   ytplaylist://playlistId=PL...      (internal playlist URL)
#   ytplaylist://channelId=UC...       (internal channel URL)
#   https://www.youtube.com/watch?v=<id>
#   https://www.youtube.com/playlist?list=PL...
#   https://music.youtube.com/playlist?list=PL...
#   https://www.youtube.com/channel/<id>
sub cliDownload {
	my $request = shift;

	if ($request->isNotCommand([['youtube'], ['download']])) {
		$request->setStatusBadDispatch();
		return;
	}

	# Accept both positional (_p2) and tagged (url:) forms.
	my $url = $request->getParam('_p2') // $request->getParam('url');

	unless ($url) {
		$log->warn('youtube download: no URL supplied');
		$request->addResultLoop('item_loop', 0, 'text', Slim::Utils::Strings::string('PLUGIN_YOUTUBE_DOWNLOAD_NO_URL'));
		$request->addResultLoop('item_loop', 0, 'type', 'text');
		$request->addResult('count', 1);
		$request->addResult('offset', 0);
		$request->setStatusDone();
		return;
	}

	my ($type, $id) = _parseUrl($url);

	unless ($type && $id) {
		$log->warn("youtube download: cannot parse URL '$url'");
		$request->addResultLoop('item_loop', 0, 'text', Slim::Utils::Strings::string('PLUGIN_YOUTUBE_DOWNLOAD_BAD_URL'));
		$request->addResultLoop('item_loop', 0, 'type', 'text');
		$request->addResult('count', 1);
		$request->addResult('offset', 0);
		$request->setStatusDone();
		return;
	}

	my $result = startDownload($type, $id);
	
	# Format as a proper menu structure that Material will display
	if ($result->{pid}) {
		# Success - show download started message with PID
		$request->addResultLoop('item_loop', 0, 'text', 
			Slim::Utils::Strings::string('PLUGIN_YOUTUBE_DOWNLOAD_STARTED'));
		$request->addResultLoop('item_loop', 0, 'type', 'text');
		
		# Extract just the URL part from the message to avoid duplication
		my ($url_part) = $result->{message} =~ /(https?:\/\/[^\s]+)/;
		$request->addResultLoop('item_loop', 1, 'text', $url_part);
		$request->addResultLoop('item_loop', 1, 'type', 'text');
		$request->addResultLoop('item_loop', 1, 'style', 'indent');
		
		$request->addResultLoop('item_loop', 2, 'text', 
			sprintf(Slim::Utils::Strings::string('PLUGIN_YOUTUBE_DOWNLOAD_PID'), $result->{pid}));
		$request->addResultLoop('item_loop', 2, 'type', 'text');
		
		$request->addResult('count', 3);
	} else {
		# Error case
		$request->addResultLoop('item_loop', 0, 'text', 
			Slim::Utils::Strings::string('PLUGIN_YOUTUBE_DOWNLOAD_FAILED'));
		$request->addResultLoop('item_loop', 0, 'type', 'text');
		
		if ($result->{message}) {
			$request->addResultLoop('item_loop', 1, 'text', $result->{message});
			$request->addResultLoop('item_loop', 1, 'type', 'text');
			$request->addResult('count', 2);
		} else {
			$request->addResult('count', 1);
		}
	}
	
	$request->addResult('offset', 0);
	$request->setStatusDone();
}

# Kick off a yt-dlp download.
# $type - 'video' or 'playlist'
# $id   - YouTube video ID  or  raw query string (playlistId=..., channelId=...)
# Returns hashref { message => '...', pid => <pid or undef> }
sub startDownload {
	my ($type, $id) = @_;

	my $binary = _ytdlpBinary();
	unless ($binary) {
		my $msg = 'yt-dlp binary not found - configure it in the YouTube plugin settings';
		$log->error($msg);
		return { message => $msg };
	}

	my $ytUrl  = _buildYtUrl($type, $id);
	my $output = _outputTemplate($type);
	my @cmd    = _buildCommand($binary, $ytUrl, $output, $type);

	# Log the resolved log file path so we can verify it in server.log
	my $logFile = _logFile();
	$log->info("yt-dlp log file will be: $logFile");
	$log->info('Starting yt-dlp download: ' . join(' ', @cmd));

	my $result = main::ISWINDOWS ? _launchWindows(@cmd) : _launchUnix($logFile, @cmd);

	if (defined $result->{pid}) {
		$log->info("Download started (pid $result->{pid}): $ytUrl");
		$result->{message} = "Download started (pid $result->{pid}): $ytUrl";
	} else {
		$log->error("Failed to launch yt-dlp for: $ytUrl");
		$result->{message} //= "Failed to launch yt-dlp for: $ytUrl";
	}

	return $result;
}

# ─── Platform launchers ─────────────────────────────────────────────────────

# Linux / macOS: fork + exec, detached from the LMS process.
#
# We cannot use AnyEvent::Util::run_cmd here (as ProtocolHandler does for
# getNextTrack) because run_cmd is designed for capturing output and driving
# a callback when the child exits.  For a long-running fire-and-forget
# download we just want a detached child; fork+exec is the right tool.
# (ProtocolHandler uses run_cmd because it needs to parse yt-dlp's JSON
# output in a non-blocking way inside the event loop; we have no such
# requirement - we just want yt-dlp running in the background.)
sub _launchUnix {
	my ($logFile, @cmd) = @_;

	# We do NOT set SIG{CHLD} in the parent — not even temporarily.
	#
	# Setting SIG{CHLD} = 'IGNORE' (even scoped with 'local') clobbers
	# AnyEvent's internal SIGCHLD watcher. LMS uses that watcher to detect
	# when the yt-dlp child started by ProtocolHandler::getNextTrack (via
	# AnyEvent::Util::run_cmd) has finished. Stomping it causes the callback
	# to never fire, leaving the playing playlist stuck between tracks.
	#
	# Zombies are not a concern: setsid() below makes the child a new session
	# leader. When it exits without a waiting parent, init (PID 1) reaps it —
	# that is standard POSIX behaviour for orphaned processes.

	my $pid = fork();

	if (!defined $pid) {
		$log->error("fork() failed: $!");
		return { message => "fork() failed: $!" };
	}

	if ($pid == 0) {
		# ── child ──
		# $logFile was resolved in the parent before fork() so we do not
		# call any LMS Perl code (OSDetect, prefs, etc.) from the child.

		# LMS ties STDIN/STDOUT/STDERR to Slim::Utils::Log::Trapper objects.
		# Perl's open() on a tied glob calls OPEN() on the tied object, which
		# Trapper does not implement. Use POSIX fd operations to bypass the
		# tied-handle layer entirely.

		# fd 0 → /dev/null
		my $null_fd = POSIX::open('/dev/null', O_RDONLY);
		POSIX::dup2($null_fd, 0) if defined $null_fd && $null_fd >= 0;
		POSIX::close($null_fd)   if defined $null_fd && $null_fd > 2;

		# fd 1 + 2 → log file, fallback chain to /tmp, /dev/null
		my $log_fd = POSIX::open($logFile, O_WRONLY | O_CREAT | O_APPEND, 0644);
		if (!defined $log_fd || $log_fd < 0) {
			$log_fd = POSIX::open('/tmp/yt-dlp-download.log',
				O_WRONLY | O_CREAT | O_APPEND, 0644);
		}
		if (!defined $log_fd || $log_fd < 0) {
			$log_fd = POSIX::open('/dev/null', O_WRONLY);
		}
		if (defined $log_fd && $log_fd >= 0) {
			my $ts = POSIX::strftime(
				"\n=== %Y-%m-%d %H:%M:%S " . join(' ', @cmd) . " ===\n",
				localtime);
			POSIX::write($log_fd, $ts, length($ts));
			POSIX::dup2($log_fd, 1);
			POSIX::dup2($log_fd, 2);
			POSIX::close($log_fd) if $log_fd > 2;
		}

		# Reset all signals to DEFAULT so yt-dlp (a PyInstaller binary) gets
		# a clean signal table and can waitpid() on its ffmpeg children.
		for my $sig (keys %SIG) {
			$SIG{$sig} = 'DEFAULT' if defined $SIG{$sig} && !ref $SIG{$sig};
		}

		# Become a new session leader, detached from LMS's process group.
		POSIX::setsid();

		exec @cmd or POSIX::_exit(1);
	}

	# ── parent ── init reaps the orphan; nothing else needed here
	return { pid => $pid };
}

# Windows: use Win32::Process::Create, mirroring exactly what ProtocolHandler
# does in getNextTrack.
#
# Why not fork()+exec() on Windows?  The same reasons ProtocolHandler
# documents in its comment at line ~626:
#   1. Windows Perl's fork() is emulated via threads and is unreliable for
#      exec()-after-fork with LMS's complex runtime.
#   2. LMS ties STDIN/STDOUT, which breaks any IPC that tries to inherit or
#      redefine them (IPC::Open2/3, AnyEvent::Util::run_cmd, etc.).
#
# Unlike getNextTrack we do NOT need to read yt-dlp's output, so we skip
# the temp-file + polling-timer dance entirely.  We just redirect
# stdout/stderr to the log file via the shell string and let it run.
sub _launchWindows {
	my ($logFile, @cmd) = @_;

	eval { require Win32::Process } or do {
		$log->error("Win32::Process not available: $@");
		return { message => "Win32::Process not available" };
	};

	# Build a shell command string: cmd.exe /c <binary> <args...> >>logfile 2>&1
	# Quote any argument that contains whitespace (paths, output templates).
	my $cmdStr = join(' ', map { /\s/ ? qq{"$_"} : $_ } @cmd);
	$cmdStr .= ' >>' . qq{"$logFile"} . ' 2>&1';

	my $proc = 0;
	eval {
		Win32::Process::Create(
			$proc,
			$ENV{COMSPEC},        # cmd.exe - same approach as ProtocolHandler
			"/c $cmdStr",
			0,                    # do NOT inherit parent handles (avoids tied-stdio issues)
			Win32::Process::NORMAL_PRIORITY_CLASS(),
			'.',                  # working directory
		);
	};

	if ($@) {
		$log->error("Win32::Process::Create failed: $@");
		return { message => "Process creation failed: $@" };
	}

	my $pid = eval { $proc->GetProcessID() } // 'unknown';
	return { pid => $pid };
}

# ─── Private helpers ────────────────────────────────────────────────────────

# Parse any supported URL form into ($type, $id).
#
# Internal LMS forms:
#   youtube://www.youtube.com/v/<id>       individual video
#   ytplaylist://playlistId=PL...          playlist
#   ytplaylist://channelId=UC...           channel
#
# Raw https:// forms (accepted from CLI/JSON-RPC):
#   https://*.youtube.com/watch?v=<id>
#   https://youtu.be/<id>
#   https://*.youtube.com/playlist?list=PL...
#   https://music.youtube.com/playlist?list=PL...
#   https://*.youtube.com/channel/<id>
#   https://*.youtube.com/c/<name>  (treated as channel)
sub _parseUrl {
	my ($url) = @_;

	# ── Internal LMS stream URL ──
	if ($url =~ m{^youtube://}i) {
		# youtube://www.youtube.com/v/<id>  or  youtube://<id>
		if ($url =~ m{^youtube://(?:(?:www|m)\.youtube\.com/v/)?([A-Za-z0-9_-]{11})(?:[^A-Za-z0-9_-]|$)}i) {
			return ('video', $1);
		}
	}

	# ── Internal LMS playlist/channel URL ──
	if ($url =~ m{^ytplaylist://(.+)}i) {
		return ('playlist', $1);
	}

	# ── Raw https:// video URL ──
	if ($url =~ m{^https?://(?:(?:www|m|music)\.youtube\.com/watch\?.*v=|youtu\.be/)([A-Za-z0-9_-]{11})}i) {
		return ('video', $1);
	}

	# ── Raw https:// playlist URL ──
	if ($url =~ m{^https?://(?:(?:www|m|music)\.youtube\.com)/playlist\?.*list=([A-Za-z0-9_-]+)}i) {
		return ('playlist', "playlistId=$1");
	}

	# ── Raw https:// channel URL ──
	if ($url =~ m{^https?://(?:(?:www|m)\.youtube\.com)/channel/([A-Za-z0-9_-]+)}i) {
		return ('playlist', "channelId=$1");
	}

	# ── Raw https:// channel vanity/custom URL — pass directly to yt-dlp ──
	if ($url =~ m{^https?://(?:(?:www|m)\.youtube\.com)/(?:c|user)/([A-Za-z0-9_-]+)}i) {
		return ('playlist', "channelId=$1");
	}

	return (undef, undef);
}

# Turn the parsed id back into a URL yt-dlp understands.
sub _buildYtUrl {
	my ($type, $id) = @_;

	if ($type eq 'video') {
		return "https://www.youtube.com/watch?v=$id";
	}

	if ($id =~ /^playlistId=(.+)$/) {
		return "https://www.youtube.com/playlist?list=$1";
	}
	if ($id =~ /^channelId=(.+)$/) {
		return "https://www.youtube.com/channel/$1";
	}

	# Fallback
	return "https://www.youtube.com/$id";
}

# Select the right output template for the item type.
sub _outputTemplate {
	my ($type) = @_;

	my $mediaFolder = _mediaFolder();

	if ($type eq 'video') {
		my $tpl = $prefs->get('download_output_video');
		return $tpl if $tpl;
		return join('/', $mediaFolder, 'YouTube', 'Singles',
			'%(uploader)s - %(title)s.%(ext)s');
	}

	# Playlist / channel
	my $tpl = $prefs->get('download_output_playlist');
	return $tpl if $tpl;

	return join('/', $mediaFolder, 'YouTube',
		'%(playlist)s',
		'%(playlist_index)03d.%(title)s.%(ext)s');
}

# Build the full yt-dlp argument list.
sub _buildCommand {
	my ($binary, $ytUrl, $output, $type) = @_;

	my @cmd = (
		$binary,
		$ytUrl,
		'-x',
		'-o', $output,
		'-f', 'bestaudio',
		'--parse-metadata', 'playlist_index:%(track_number)s',
		'--parse-metadata', '%(release_date,upload_date)s:(?P<meta_date>[0-9]{4})',
		'--embed-metadata',
		'--embed-thumbnail',
		'--convert-thumbnails', 'jpg',
		'--postprocessor-args',
			'ThumbnailsConvertor:-vf scale=500:500:force_original_aspect_ratio=increase,crop=500:500',
	);

	# Keep going even if one video in the list fails
	push @cmd, '--no-abort-on-error' if $type eq 'playlist';

	return @cmd;
}

# Locate the yt-dlp binary.
# Delegates to Utils::yt_dlp_bin(), exactly as ProtocolHandler::getNextTrack does.
# That function resolves the stored pref value (a bare filename like "yt-dlp_linux")
# against the plugin Bin/ directory, producing the correct full path.
sub _ytdlpBinary {
	return Plugins::YouTube::Utils::yt_dlp_bin( $prefs->get('yt_dlp') );
}

# Return the LMS media folder root.
sub _mediaFolder {
	my $custom = $prefs->get('download_media_folder');
	return $custom if $custom && -d $custom;

	my $serverPrefs = preferences('server');
	my $dirs = $serverPrefs->get('audiodir');
	$dirs    = $serverPrefs->get('mediadirs') unless $dirs;

	if (ref $dirs eq 'ARRAY') {
		return $dirs->[0] if @$dirs;
	} elsif ($dirs) {
		return $dirs;
	}

	# Last resort: OS default music folder
	return (Slim::Utils::OSDetect::dirsFor('music'))[0] // '.';
}

# Path to the dedicated yt-dlp download log.
sub _logFile {
	# Slim::Utils::OSDetect::dirsFor('log') is the canonical LMS API for
	# the log directory -- the same mechanism used throughout the server.
	# We avoid Slim::Utils::Log::serverLogFile() because it is not
	# guaranteed to exist in all LMS versions.
	my ($logDir) = Slim::Utils::OSDetect::dirsFor('log');
	return File::Spec->catfile($logDir, 'yt-dlp-download.log');
}

1;