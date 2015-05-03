#!/usr/bin/env python
# -*- coding: utf-8 -*-
__appname__ = 'gogrepo.py'
__author__ = 'eddie3'
__version__ = '0.2'
__url__ = 'https://github.com/eddie3/gogrepo'

# imports
import os
import sys
import threading
import Queue
import logging
import contextlib
import cookielib
import urllib
import urllib2
import urlparse
import json
import html5lib
import httplib
import pprint
import time
import zipfile
import hashlib
import getpass
import argparse
import codecs
import datetime
import shutil
import socket
import xml.etree.ElementTree

# configure logging
logFormatter = logging.Formatter("%(asctime)s | %(message)s", datefmt='%H:%M:%S')
rootLogger = logging.getLogger('ws')
rootLogger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

# logging aliases
info = rootLogger.info
warn = rootLogger.warning
debug = rootLogger.debug
error = rootLogger.error
log_exception = rootLogger.exception

# filepath constants
GAME_STORAGE_DIR = r'.'
COOKIES_FILENAME = r'gog-cookies.dat'
MANIFEST_FILENAME = r'gog-manifest.dat'
SERIAL_FILENAME = r'!serial.txt'
INFO_FILENAME = r'!info.txt'

# global web utilities
cookiejar = cookielib.LWPCookieJar(COOKIES_FILENAME)
cookieproc = urllib2.HTTPCookieProcessor(cookiejar)
opener = urllib2.build_opener(cookieproc)
treebuilder = html5lib.treebuilders.getTreeBuilder('etree')
parser = html5lib.HTMLParser(tree=treebuilder, namespaceHTMLElements=False)

# GOG URLs
GOG_HOME_URL = r'http://www.gog.com'
GOG_AJAX_URL = r'https://www.gog.com/account/ajax'
GOG_LOGIN_URL = r'https://login.gog.com/login_check'

# HTTP request settings
HTTP_FETCH_DELAY = 1   # in seconds
HTTP_RETRY_DELAY = 5   # in seconds
HTTP_RETRY_COUNT = 3
HTTP_GAME_DOWNLOADER_THREADS = 4
HTTP_PERM_ERRORCODES = (404, 403, 503)

# Save manifest data for these os and lang combinations
DEFAULT_OS_LIST = ['win']
DEFAULT_LANG_LIST = ['en']


def request(url, args=None, byte_range=None, retries=HTTP_RETRY_COUNT, delay=HTTP_FETCH_DELAY):
    """Performs web request to url with optional retries, delay, and byte range.
    """
    _retry = False
    time.sleep(delay)

    try:
        if args is not None:
            enc_args = urllib.urlencode(args)
        else:
            enc_args = None
        req = urllib2.Request(url, data=enc_args)
        if byte_range is not None:
            req.add_header('Range', 'bytes=%d-%d' % byte_range)
        page = opener.open(req)
    except (urllib2.HTTPError, urllib2.URLError, socket.error, httplib.BadStatusLine) as e:
        if isinstance(e, urllib2.HTTPError):
            if e.code in HTTP_PERM_ERRORCODES:  # do not retry these HTTP codes
                warn('request failed: %s. will not retry.', e)
                raise
        if retries > 0:
            _retry = True
        else:
            raise

    if _retry:
        warn('request failed: %s (%d retries left) -- will retry in %ds...' %
             (e, retries, HTTP_RETRY_DELAY))
        return request(url=url, args=args, byte_range=byte_range, retries=retries-1, delay=HTTP_RETRY_DELAY)

    return contextlib.closing(page)


# --------------------------
# Helper types and functions
# --------------------------
class AttrDict(dict):
    def __init__(self, **kw):
        self.update(kw)

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, val):
        self[key] = val


def load_attrdicts(fn):
    try:
        with open(fn, 'rU') as r:
            ad = r.read().replace('{', 'AttrDict(**{').replace('}', '})')
        return eval(ad)
    except IOError:
        return AttrDict()


def load_cookies():
    try:
        cookiejar.load()
    except IOError:
        pass


def load_manifest():
    info('loading local manifest...')
    return load_attrdicts(MANIFEST_FILENAME)


def open_notrunc(name, bufsize=4*1024):
    flags = os.O_WRONLY | os.O_CREAT
    if hasattr(os, "O_BINARY"):
        flags |= os.O_BINARY  # windows
    fd = os.open(name, flags, 0666)
    return os.fdopen(fd, 'wb', bufsize)


def hashfile(afile, blocksize=65536):
    afile = open(afile, 'rb')
    hasher = hashlib.md5()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    return hasher.hexdigest()


def test_zipfile(filename):
    """Opens filename and tests the file for ZIP integrity.  Returns True if
    zipfile passes the integrity test, False otherwise.
    """
    try:
        with zipfile.ZipFile(filename, 'r') as f:
            if f.testzip() is None:
                return True
    except zipfile.BadZipfile:
        return False

    return False


def fetch_game_shelf_data():
    full_game_list_html = []
    all_games_fetched = False
    i = 0

    # request each game shelf page until we have all game json data
    while not all_games_fetched:
        i += 1  # starts at page 1
        info('downloading game shelf data (page %d)...' % i)
        with request(GOG_AJAX_URL, args={'a': 'gamesShelfMore',
                                         's': 'title',  # sort order
                                         'q': '',
                                         't': '0',
                                         'p': str(i)}, delay=0) as data_request:
            game_data = json.load(data_request)

            if game_data['count'] == 0:
                all_games_fetched = True
            else:
                full_game_list_html.append(game_data['html'])  # append new html payload

    html_txt = u''.join(full_game_list_html)

    return html_txt


def process_argv(argv):
    p1 = argparse.ArgumentParser(description='%s (%s)' % (__appname__, __url__), add_help=False)
    sp1 = p1.add_subparsers(help='commands', dest='cmd', title='commands')

    g1 = sp1.add_parser('login', help='Login to GOG and save a local copy of your authenticated cookie')
    g1.add_argument('username', action='store', help='GOG username/email', nargs='?', default=None)
    g1.add_argument('password', action='store', help='GOG password', nargs='?', default=None)

    g1 = sp1.add_parser('update', help='Update locally saved game manifest from GOG server')
    g1.add_argument('-os', action='store', help='operating system(s)', nargs='*', default=DEFAULT_OS_LIST)
    g1.add_argument('-lang', action='store', help='game language(s)', nargs='*', default=DEFAULT_LANG_LIST)

    g1 = sp1.add_parser('download', help='Download all your GOG games and bonus files')
    g1.add_argument('savedir', action='store', help='directory to save downloads to', nargs='?', default='.')
    g1.add_argument('-dryrun', action='store_true', help='display, but skip downloading of any files')
    g1.add_argument('-skipbonus', action='store_true', help='skip downloading of any GOG bonus files')
    g1.add_argument('-wait', action='store', type=float,
                    help='wait this long in hours before starting', default=0.0)  # sleep in hr

    g1 = sp1.add_parser('import', help='Import files with any matching MD5 checksums found in manifest')
    g1.add_argument('src_dir', action='store', help='source directory to import games from')
    g1.add_argument('dest_dir', action='store', help='directory to copy and name imported files to')

    g1 = sp1.add_parser('backup', help='Perform an incremental backup to specified directory')
    g1.add_argument('src_dir', action='store', help='source directory containing gog items')
    g1.add_argument('dest_dir', action='store', help='destination directory to backup files to')

    g1 = sp1.add_parser('verify', help='Scan your downloaded GOG files and verify their size, MD5, and zip integrity')
    g1.add_argument('gamedir', action='store', help='directory containing games to verify', nargs='?', default='.')
    g1.add_argument('-skipmd5', action='store_true', help='do not perform MD5 check')
    g1.add_argument('-skipsize', action='store_true', help='do not perform size check')
    g1.add_argument('-skipzip', action='store_true', help='do not perform zip integrity check')
    g1.add_argument('-delete', action='store_true', help='delete any files which fail integrity test')

    g1 = p1.add_argument_group('other')
    g1.add_argument('-h', '--help', action='help', help='show help message and exit')
    g1.add_argument('-v', '--version', action='version', help='show version number and exit',
                    version="%s (version %s)" % (__appname__, __version__))

    # parse the given argv.  raises SystemExit on error
    return p1.parse_args(argv[1:])


# --------
# Commands
# --------
def cmd_login(user, passwd):
    """Attempts to log into GOG and saves the resulting cookiejar to disk.

    If passwd is None, the user will be prompted for one in the console.
    """
    if user is None:
        user = getpass.getpass("enter username: ")
    if passwd is None:
        passwd = getpass.getpass("enter password: ")

    cookiejar.clear()  # reset cookiejar

    # get the auth_url
    info("attempting gog login as '%s' ..." % user)
    with request(GOG_HOME_URL, delay=0) as web_data:
        etree = html5lib.parse(web_data, namespaceHTMLElements=False)
    for elm in etree.findall('.//script'):
        if elm.text is not None and 'GalaxyAccounts' in elm.text:
            auth_url = elm.text.split("'")[1]
            break

    # request auth_url and find the login_token
    with request(auth_url, delay=0) as page:
        etree = html5lib.parse(page, namespaceHTMLElements=False)

        for elm in etree.findall('.//input'):
            if elm.attrib['id'] == 'login__token':
                login_token = elm.attrib['value']
                break

    # perform the login
    request(GOG_LOGIN_URL, delay=0, args={'login[username]': user,
                                          'login[password]': passwd,
                                          'login[login]': '',
                                          'login[_token]': login_token})

    # save cookies to disk
    cookiejar.save()

    # verify login was successful
    for c in cookiejar:
        if c.name == 'galaxy-login-al':
            info('login successful!')
            return

    error('login failed, verify your username/password and try again.')


def cmd_update(os_list, lang_list):
    gamesdb = {}  # represents the AttrDict blob to eventually be saved to disk

    load_cookies()

    info('downloading game shelf data...')
    shelf_html = fetch_game_shelf_data()

    info('parsing html for game data...')
    shelf_etree = parser.parse(shelf_html)
    games_tree = shelf_etree.findall(".//div[@class='shelf_game']")
    games_count = len(games_tree)
    print_padding = len(str(games_count))
    info('found %d games !!%s' % (games_count, '!'*(games_count/100)))  # teehee

    i = 0
    for game in games_tree:
        g = AttrDict()
        i += 1

        g.title = game.attrib['data-gameindex'].strip()

        info("(%*d / %d) fetching game info for %s..." % (print_padding, i, games_count, g.title))

        g.game_id = game.attrib['data-gameid'].strip()
        g.background_url = game.attrib['data-background'].strip()
        g.keywords = game.attrib['data-title'].strip()
        g.cover_url = game.find(".//img[@src]").attrib['src']

        # fetch detailed game information
        with request(GOG_AJAX_URL, args={'a': 'gamesShelfDetails',
                                         'g': g.game_id}) as data_request:
            game_data = json.load(data_request)
        game_element = parser.parse(game_data['details']['html'])

        try:
            g.long_title = game_element.find(".//h2/a").text.strip()
        except:
            g.long_title = game_element.find(".//h2").text.strip()  # <-- some games do not have <a> tag

        g.purchased = game_element.find(".//div[@class='list_purchased']").text.replace('Purchased on', '').strip()

        # collect serial/cd key if the game has one
        g.serial = ''
        for elm in game_element.findall(".//div[@class='list_serial_h']"):
            for serial_txt in elm.itertext():
                g.serial += "%s\n" % serial_txt.strip()
            g.serial = "%s\n" % g.serial.strip()

        # collect game items in permutations of OS and Language
        g.game_items = []
        for lang in lang_list:
            for os_type in os_list:
                for dl_link in game_element.findall(".//div[@class='%s-download dldl']"
                                                    "/div[@class='lang-item lang_%s']/a[@class='list_game_item']"
                                                    % (os_type, lang)):
                    g.game_items.append(AttrDict(
                        os_type=os_type,
                        lang=lang,
                        href=dl_link.attrib['href'].strip(),
                        desc=dl_link.find("./span[@class='details-underline']").text.strip())
                    )

        # collect bonus material
        g.bonus_items = []
        for dl_link in game_element.findall(".//div[@class='bonus_content_list browser']/a"):
            g.bonus_items.append(AttrDict(
                os_type='bonus',
                lang=None,
                href=dl_link.attrib['href'].strip(),
                desc=dl_link.find("./span[@class='details-underline']").text.strip())
            )

        gamesdb[g.game_id] = g

    # fetch md5 and size info for each file
    loop_cnt = 0
    for g in sorted(gamesdb.values(), key=lambda g: g.title):
        loop_cnt += 1
        # determine the size, name, and url of the download
        info('(%3d / %d) fetching md5 for %s...' % (loop_cnt, games_count, g.title))
        for i, f in enumerate(g.game_items + g.bonus_items):
            f.name = None
            f.size = None
            f.url = None
            try:
                with request(f.href, byte_range=(0, 0)) as page:
                    f.name = urlparse.urlparse(page.geturl()).path.split('/')[-1]
                    f.size = int(page.headers['Content-Range'].split('/')[-1])
                    if i < len(g.game_items):  # skip bonus items, as they have no md5 data
                        f.url = page.geturl()
            except urllib2.HTTPError:
                warn("failed to fetch %s. skipping..." % f.href)

        # fetch the md5 file and chunk data for each game
        for f in sorted(g.game_items, key=lambda g: g.name):
            try:
                if f.url is not None:
                    with request(f.url.replace('?', '.xml?')) as page:
                        shelf_etree = xml.etree.ElementTree.parse(page).getroot()
            except urllib2.HTTPError:
                warn("failed to fetch md5 data for %s. skipping..." % f.name)
                continue
            finally:
                del f['url']

            # make sure name and size matched what we expected
            if f.name is not None:
                assert f.name == shelf_etree.attrib['name']
            if f.size is not None:
                if f.size != int(shelf_etree.attrib['total_size']):
                    error("%s manifest size mismatch detected (f.size=%d, total_size=%d)"
                          % (f.name,
                             f.size,
                             int(shelf_etree.attrib['total_size'])))

            f.md5 = shelf_etree.attrib['md5']
            f.part = [(int(chunk.attrib['from']),
                       int(chunk.attrib['to']),
                       chunk.text)
                      for chunk in shelf_etree.findall(".//chunk[@method='md5']")]
            assert len(f.part) == int(shelf_etree.attrib['chunks'])
            f.part.sort()

    # write all this meta-data to disk
    with open(MANIFEST_FILENAME, 'w') as w:
        print >>w, '# %d games' % len(gamesdb)
        pprint.pprint(gamesdb.values(), width=123, stream=w)


def cmd_import(src_dir, dest_dir):
    """Recursively finds all files within root_dir and compares their MD5 values
    against known md5 values from the manifest.  If a match is found, the file will be copied
    into the game storage dir.
    """
    gamesdb = load_manifest()

    info("collecting md5 data out of the manifest")
    md5_info = {}  # holds tuples of (title, filename)

    for game in gamesdb:
        for game_item in game.game_items:
            try:
                md5_info[game_item.md5] = (game.title, game_item.name)
            except KeyError:  # some files don't have md5's in the manifest
                pass

    info("searching for files within '%s'" % src_dir)
    file_list = []
    for (root, dirnames, filenames) in os.walk(src_dir):
        for f in filenames:
            if not f.lower().endswith('.zip'):  # skip .zip files, they never have md5 data in manifest
                file_list.append(os.path.join(root, f))

    info("comparing md5 file hashes")
    for f in file_list:
        fname = os.path.basename(f)
        info("calculating md5 for '%s'" % fname)
        h = hashfile(f)
        if h in md5_info:
            title, fname = md5_info[h]
            src_dir = os.path.join(dest_dir, title)
            dest_file = os.path.join(src_dir, fname)
            info('found a match! [%s] -> %s' % (h, fname))
            if os.path.isfile(dest_file):
                if h == hashfile(dest_file):
                    info('destination file already exists with the same md5 value.  skipping copy.')
                    continue
            info("copying to %s..." % dest_file)
            if not os.path.isdir(src_dir):
                os.makedirs(src_dir)
            shutil.copy(f, dest_file)


def cmd_download(savedir, skipbonus, dryrun):
    sizes, rates, errors = {}, {}, {}
    work = Queue.Queue()  # build a list of work items

    load_cookies()

    gamesdb = load_manifest()

    # Find all items to be downloaded and push into work queue
    for game in sorted(gamesdb, key=lambda g: g.title):
        info("{%s}" % game.title)
        game_homedir = os.path.join(savedir, game.title)
        if not dryrun:
            if not os.path.isdir(game_homedir):
                os.makedirs(game_homedir)

        if skipbonus:
            game.bonus_items = []

        # Generate and save a game info text file
        if not dryrun:
            with codecs.open(os.path.join(game_homedir, INFO_FILENAME), 'w', 'utf-8') as fd_info:
                fd_info.write('\n-- %s -- \n\n' % game.long_title)
                fd_info.write('title.......... %s\n' % game.title)
                fd_info.write('keywords....... %s\n' % game.keywords)
                fd_info.write('purchased...... %s\n' % game.purchased)
                fd_info.write('game id........ %s\n' % game.game_id)
                fd_info.write('\ngame items.....\n')
                for game_item in game.game_items:
                    fd_info.write('    [%s] -- %s\n' % (game_item.name, game_item.desc))
                if len(game.bonus_items) > 0:
                    fd_info.write('\nbonus items....\n')
                    for game_item in game.bonus_items:
                        fd_info.write('    [%s] -- %s\n' % (game_item.name, game_item.desc))

        # Generate and save a game serial text file
        if not dryrun:
            if game.serial != '':
                with codecs.open(os.path.join(game_homedir, SERIAL_FILENAME), 'w', 'utf-8') as fd_serial:
                    fd_serial.write(game.serial)

        # Populate queue with all files to be downloaded
        for game_item in game.game_items + game.bonus_items:
            if game_item.name is None:
                continue  # no game name, usually due to 404 during file fetch
            dest_file = os.path.join(game_homedir, game_item.name)

            if os.path.isfile(dest_file):
                if game_item.size is None:
                    warn('     unknown    %s has no size info.  skipping')
                    continue
                elif game_item.size != os.path.getsize(dest_file):
                    warn('     fail       %s has incorrect size.' % game_item.name)
                else:
                    info('     pass       %s' % game_item.name)
                    continue  # move on to next game item

            info('     download   %s' % game_item.name)
            sizes[dest_file] = game_item.size

            work.put((game_item.href, game_item.size, 0, game_item.size-1, dest_file))

    if dryrun:
        return  # bail, as below just kicks off the actual downloading

    info('-'*60)

    # util
    def megs(b):
        return '%.1fMB' % (b / float(1024**2))
    def gigs(b):
        return '%.2fGB' % (b / float(1024**3))

    # work item I/O loop
    def ioloop(tid, path, page, out):
        sz, t0 = True, time.time()
        while sz:
            buf = page.read(4*1024)
            t = time.time()
            out.write(buf)
            sz, dt, t0 = len(buf), t - t0, t
            with lock:
                sizes[path] -= sz
                rates.setdefault(path, []).append((tid, (sz, dt)))

    # downloader worker thread main loop
    def worker():
        tid = threading.current_thread().ident
        while not work.empty():
            (href, sz, start, end, path) = work.get()
            try:
                dest_dir = os.path.dirname(path)
                with lock:
                    if not os.path.isdir(dest_dir):
                        os.makedirs(dest_dir)
                with open_notrunc(path) as out:
                    out.seek(start)
                    se = start, end
                    try:
                        with request(href, byte_range=se) as page:
                            hdr = page.headers['Content-Range'].split()[-1]
                            if hdr != '%d-%d/%d' % (start, end, sz):
                                with lock:
                                    error("chunk request has unexpected Content-Range. "
                                          "expected '%d-%d/%d' received '%s'. skipping."
                                          % (start, end, sz, hdr))
                            else:
                                assert out.tell() == start
                                ioloop(tid, path, page, out)
                                assert out.tell() == end + 1
                    except urllib2.HTTPError as e:
                        error("failed to download %s, byte_range=%s" % (os.path.basename(path), str(se)))
            except IOError, e:
                with lock:
                    print >>sys.stderr, '!', path
                    errors.setdefault(path, []).append(e)
            work.task_done()

    # detailed progress report
    def progress():
        with lock:
            left = sum(sizes.values())
            for path, flowrates in sorted(rates.items()):
                flows = {}
                for tid, (sz, t) in flowrates:
                    szs, ts = flows.get(tid, (0, 0))
                    flows[tid] = sz + szs, t + ts
                bps = sum(szs/ts for szs, ts in flows.values() if ts > 0)
                info('%10s %8.1fMB/s %2dx  %s' % \
                    (megs(sizes[path]), bps / 1024.0**2, len(flows), "%s/%s" % (os.path.basename(os.path.split(path)[0]), os.path.split(path)[1])))
            if len(rates) != 0:  # only update if there's change
                info('%s remaining' % gigs(left))
            rates.clear()

    # process work items with a thread pool
    lock = threading.Lock()
    pool = []
    for i in range(HTTP_GAME_DOWNLOADER_THREADS):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        pool.append(t)
    try:
        while any(t.is_alive() for t in pool):
            progress()
            time.sleep(1)
    except KeyboardInterrupt:
        raise
    except:
        with lock:
            log_exception('')
        raise


def cmd_backup(src_dir, dest_dir):
    gamesdb = load_manifest()

    info('finding all known files in the manifest')
    for game in sorted(gamesdb, key=lambda g: g.title):
        touched = False
        for itm in game.game_items + game.bonus_items:
            if itm.name is None:
                continue

            src_game_dir = os.path.join(src_dir, game.title)
            src_file = os.path.join(src_game_dir, itm.name)
            dest_game_dir = os.path.join(dest_dir, game.title)
            dest_file = os.path.join(dest_game_dir, itm.name)

            if os.path.isfile(src_file):
                if itm.size != os.path.getsize(src_file):
                    warn('source file %s has unexpected size. skipping.' % src_file)
                    continue
                if not os.path.isdir(dest_game_dir):
                    os.makedirs(dest_game_dir)
                if not os.path.exists(dest_file) or itm.size != os.path.getsize(dest_file):
                    info('copying to %s...' % dest_file)
                    shutil.copy(src_file, dest_file)
                    touched = True

        # backup the info and serial files too
        if touched and os.path.isdir(dest_game_dir):
            for extra_file in [INFO_FILENAME, SERIAL_FILENAME]:
                if os.path.exists(os.path.join(src_game_dir, extra_file)):
                    shutil.copy(os.path.join(src_game_dir, extra_file), dest_game_dir)


def cmd_verify(gamedir, check_md5, check_filesize, check_zips, delete_on_fail):
    """Verifies all game files match manifest with any available md5 & file size info
    """
    item_count = 0
    missing_cnt = 0
    bad_md5_cnt = 0
    bad_size_cnt = 0
    bad_zip_cnt = 0
    del_file_cnt = 0

    gamesdb = load_manifest()

    info('verifying all known files in the manifest')
    for game in sorted(gamesdb, key=lambda g: g.title):
        for itm in game.game_items + game.bonus_items:
            if itm.name is None:
                warn('no known filename for "%s (%s)"' % (game.title, itm.desc))
                continue

            item_count += 1

            itm_dirpath = os.path.join(game.title, itm.name)
            itm_file = os.path.join(gamedir, game.title, itm.name)

            if os.path.isfile(itm_file):
                info('verifying %s...' % itm_dirpath)
                if not hasattr(itm, 'md5') and not hasattr(itm, 'size'):
                    warn('no md5 or size info found for %s' % itm_dirpath)

                fail = False
                if check_md5 and hasattr(itm, 'md5'):
                    if itm.md5 != hashfile(itm_file):
                        info('mismatched md5 for %s' % itm_dirpath)
                        bad_md5_cnt += 1
                        fail = True
                if check_filesize and hasattr(itm, 'size'):
                    if itm.size != os.path.getsize(itm_file):
                        info('mismatched file size for %s' % itm_dirpath)
                        bad_size_cnt += 1
                        fail = True
                if check_zips and itm.name.lower().endswith('.zip'):
                    if not test_zipfile(itm_file):
                        info('zip test failed for %s' % itm_dirpath)
                        bad_zip_cnt += 1
                if delete_on_fail and fail:
                    info('deleting %s' % itm_dirpath)
                    os.remove(itm_file)
                    del_file_cnt += 1
            else:
                info('missing file %s' % itm_dirpath)
                missing_cnt += 1

    info('')
    info('--totals------------')
    info('items in manifest... %d' % item_count)
    info('have items.......... %d' % (item_count - missing_cnt - del_file_cnt))
    info('missing items....... %d' % (missing_cnt + del_file_cnt))
    if check_md5:
        info('md5 mismatches...... %d' % bad_md5_cnt)
    if check_filesize:
        info('size mismatches..... %d' % bad_size_cnt)
    if check_zips:
        info('zipfile failures.... %d' % bad_zip_cnt)
    if delete_on_fail:
        info('deleted items....... %d' % del_file_cnt)


def main(args):
    stime = datetime.datetime.now()

    if args.cmd == 'login':
        cmd_login(args.username, args.password)
        return  # no need to see time stats
    elif args.cmd == 'update':
        cmd_update(args.os, args.lang)
    elif args.cmd == 'download':
        if args.wait > 0.0:
            info('sleeping for %.2fhr...' % args.wait)
            time.sleep(args.wait * 60 * 60)
        cmd_download(args.savedir, args.skipbonus, args.dryrun)
    elif args.cmd == 'import':
        cmd_import(args.src_dir, args.dest_dir)
    elif args.cmd == 'verify':
        check_md5 = not args.skipmd5
        check_filesize = not args.skipsize
        check_zips = not args.skipzip
        cmd_verify(args.gamedir, check_md5, check_filesize, check_zips, args.delete)
    elif args.cmd == 'backup':
        cmd_backup(args.src_dir, args.dest_dir)

    etime = datetime.datetime.now()
    info('--')
    info('total time: %s' % (etime - stime))


if __name__ == "__main__":
    try:
        main(process_argv(sys.argv))
    except KeyboardInterrupt:
        sys.exit(1)
    except SystemExit:
        raise
    except:
        log_exception('fatal...')
        sys.exit(1)