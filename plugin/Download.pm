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

# CLI handler:  youtube download url:<url>
sub cliDownload {
	my $request = shift;

	if ($request->isNotCommand([['youtube'], ['download']])) {
		$request->setStatusBadDispatch();
		return;
	}

	my $url = $request->getParam('_url') // $request->getParam('url');

	unless ($url) {
		$log->warn('youtube download: no URL supplied');
		$request->addResult('error', 'No URL supplied');
		$request->setStatusDone();
		return;
	}

	my ($type, $id) = _parseUrl($url);

	unless ($type && $id) {
		$log->warn("youtube download: cannot parse URL '$url'");
		$request->addResult('error', "Cannot parse URL: $url");
		$request->setStatusDone();
		return;
	}

	my $result = startDownload($type, $id);
	$request->addResult('result', $result->{message});
	$request->addResult('pid',    $result->{pid}) if $result->{pid};
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

	# Must be set BEFORE fork() to close the race where the child exits
	# between fork() returning and the parent reaching this line, which
	# would leave a zombie. 'local' unwinds it when the sub returns.
	local $SIG{CHLD} = 'IGNORE';

	my $pid = fork();

	if (!defined $pid) {
		$log->error("fork() failed: $!");
		return { message => "fork() failed: $!" };
	}

	if ($pid == 0) {
		# ── child ──
		# $logFile was resolved in the parent before fork() so we do not
		# call any LMS Perl code (OSDetect, prefs, etc.) from the child.
		# That avoids any tied-handle or module-state issues post-fork.

		# LMS ties STDIN/STDOUT/STDERR to Slim::Utils::Log::Trapper objects.
		# Perl's open() on a tied glob calls OPEN() on the tied object, which
		# Trapper does not implement. Bypass the tied-handle layer entirely
		# by working at the POSIX file-descriptor level.

		# fd 0 → /dev/null
		my $null_fd = POSIX::open('/dev/null', O_RDONLY);
		POSIX::dup2($null_fd, 0) if defined $null_fd && $null_fd >= 0;
		POSIX::close($null_fd)   if defined $null_fd && $null_fd > 2;

		# fd 1 + 2 → yt-dlp log file (append)
		# Try the resolved path first, then /tmp, then /dev/null.
		my $log_fd = POSIX::open($logFile, O_WRONLY | O_CREAT | O_APPEND, 0644);
		if (!defined $log_fd || $log_fd < 0) {
			$log_fd = POSIX::open('/tmp/yt-dlp-download.log',
				O_WRONLY | O_CREAT | O_APPEND, 0644);
		}
		if (!defined $log_fd || $log_fd < 0) {
			$log_fd = POSIX::open('/dev/null', O_WRONLY);
		}
		if (defined $log_fd && $log_fd >= 0) {
			# Write a timestamp header before handing the fd to yt-dlp.
			# We use the raw fd via syswrite so we stay below Perl's tied
			# stdio layer. strftime is imported from POSIX at the top.
			my $ts = POSIX::strftime(
				"\n=== %Y-%m-%d %H:%M:%S " . join(' ', @cmd) . " ===\n",
				localtime);
			POSIX::write($log_fd, $ts, length($ts));

			POSIX::dup2($log_fd, 1);
			POSIX::dup2($log_fd, 2);
			POSIX::close($log_fd) if $log_fd > 2;
		}

		# Reset SIGCHLD to default before exec so that yt-dlp (a frozen
		# PyInstaller binary) can waitpid() on its own ffmpeg children.
		# We set IGNORE in the parent before fork() to prevent zombies, but
		# that disposition is inherited across exec and breaks yt-dlp's
		# internal child-process handling (visible as PYI LOADER warnings).
		$SIG{CHLD} = 'DEFAULT';

		# Detach from LMS signal group.
		POSIX::setsid();

		exec @cmd or POSIX::_exit(1);
	}

	# ── parent ──
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

# Parse a youtube:// or ytplaylist:// URL into ($type, $id).
# Mirrors the regex logic from ProtocolHandler::getId().
sub _parseUrl {
	my ($url) = @_;

	# Individual video: youtube://www.youtube.com/v/<id>  or  youtube://<id>
	if ($url =~ m{^youtube://(?:(?:www|m)\.youtube\.com/v/)?([A-Za-z0-9_-]{11})}i ||
	    $url =~ m{^youtube://([A-Za-z0-9_-]{11})}i) {
		return ('video', $1);
	}

	# Playlist or channel: ytplaylist://playlistId=PL... | ytplaylist://channelId=UC...
	if ($url =~ m{^ytplaylist://(.+)}i) {
		return ('playlist', $1);   # keep the raw key=value string
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