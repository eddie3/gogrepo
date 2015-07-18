gogrepo
-------
Python-based tool for downloading your GOG.com game collections and extras to your local computer for full offline enjoyment.

It is a clean standalone python script that can be run from anywhere. It requires a typical Python 2.7 installation and html5lib.

By default, game folders are saved in the same location that the script is run in. You can also specify another
directory. Run gogrepo.py -h to see help or read more below. Each game has its own directories with all game/bonus files saved within.

License: GPLv3+

Features
--------
* Ability to choose which games to download based on combinations of OS (windows, linux, mac) and language (en, fr, de, etc...)
* Saves a !info.txt in each game folder with information about each game/extra item.
* Creates a !serial.txt if the game has a special serial/cdkey (I know, not 100% DRM-free, is it?). Sometimes coupon codes are hidden here!
* Verify your downloaded collection with full MD5, zip integrity, and expected file size checking.
* Auto retrying of failed fetch/downloads. Sometime GOG servers report temporary errors.
* Ability to import your already existing local collection.
* Easy to throw into a daily cronjob to get all the latest updates and newly added content!
* Clear logging prints showing update/download progress and HTTP errors. Easy to pipe or tee to create a log file.


Quick Start -- Typical Use Case
----------------

* Login to GOG and save your login cookie for later commands. Your login/pass can be specified or be prompted. You generally only need to do this once to create a valid gog-cookies.dat

  ``gogrepo.py login``

* Fetch all game and bonus information from GOG for items that you own and save into a local manifest file. Run this whenever you want to discover newly added games or game updates.

  ``gogrepo.py update -os windows linux mac -lang en de fr``

* Download the games and bonus files for the OS and languages you want for all items known from the saved manifest file.

  ``gogrepo.py download``

* Verify and report integrity of all downloaded files. Does MD5, zip integrity, and expected filesize verification. This makes sure your game files can actually be read back and are healthy.

  ``gogrepo.py verify``

Advanced Usage -- Common Tasks
----------------

* Add new games from your library to the manifest.

  ``gogrepo.py update -os windows -lang en de -skipknown``

* Update games with the updated tag in your libary.

  ``gogrepo.py update -os windows -lang en de -updateonly``

* Update a single game in your manifest.

  ``gogrepo.py update -os windows -lang en de -id trine_2_complete_story``

* Download a single game in your manifest.

  ``gogrepo.py download -id trine_2_complete_story``

Commands
--------

``gogrepo.py login`` Authenticate with GOG and save the cookie locally in gog-cookies.dat file. This is needed to do
update or download command. Run this once first before doing update and download.

    login [-h] [username] [password]
    -h, --help  show this help message and exit
    username    GOG username/email
    password    GOG password

--

``gogrepo.py update`` Fetch game data and information from GOG.com for the specified operating systems and languages. This collects file game titles, download links, serial numbers, MD5/filesize data and saves the data locally in a manifest file. Manifest is saved in a gog-manifest.dat file

    update [-h] [-os [OS [OS ...]]] [-lang [LANG [LANG ...]]] [-skipknown | -updateonly | -id <title>]
    -h, --help            show this help message and exit
    -os [OS [OS ...]]     operating system(s) (ex. windows linux mac)
    -lang [LANG [LANG ...]]  game language(s) (ex. en fr de)
    -skipknown            only update new games in your library
    -updateonly           only update games with the updated tag in your library
    -id <title>           specify the game to update by 'title' from the manifest
                          <title> can be found in the !info.txt of the game directory

--

``gogrepo.py download`` Use the saved manifest file from an update command, and download all known game items and bonus files.

    download [-h] [-dryrun] [-skipextras] [-skipextras] [-skipgames] [-wait WAIT] [-id <title>] [savedir]
    -h, --help   show this help message and exit
    -dryrun      display, but skip downloading of any files
    -skipextras  skip downloading of any GOG extra files
    -skipgames   skip downloading of any GOG game files
    -wait WAIT   wait this long in hours before starting
    -id <title>  specify the game to download by 'title' from the manifest
                 <title> can be found in the !info.txt of the game directory
    savedir      directory to save downloads to

--

``gogrepo.py verify`` Check all your game files against the save manifest data, and verify MD5, zip integrity, and
expected file size. Any missing or corrupt files will be reported.

    verify [-h] [-skipmd5] [-skipsize] [-skipzip] [-delete] [gamedir]
    gamedir     directory containing games to verify
    -h, --help  show this help message and exit
    -skipmd5    do not perform MD5 check
    -skipsize   do not perform size check
    -skipzip    do not perform zip integrity check
    -delete     delete any files which fail integrity test

--

``gogrepo.py import`` Search an already existing GOG collection for game item/files, and import them to your
new GOG folder with clean game directory names and file names as GOG has them named on their servers.

    import [-h] src_dir dest_dir
    src_dir     source directory to import games from
    dest_dir    directory to copy and name imported files to
    -h, --help  show this help message and exit

--

``gogrepo.py backup`` Make copies of all known files in manifest file from a source directory to a backup destination directory. Useful for cleaning out older files from your GOG collection.

    backup [-h] src_dir dest_dir
    src_dir     source directory containing gog items
    dest_dir    destination directory to backup files to
    -h, --help  show this help message and exit


Requirements
------------
* Python 2.7 (Python 3 support coming soon)
* html5lib 0.99999 (https://github.com/html5lib/html5lib-python)
* html2text 2015.6.21 (https://pypi.python.org/pypi/html2text) (optional, used for prettying up gog game changelog html)

I recommend you use `pip` to install the above python modules. 

  ``pip install html5lib html2text``

TODO
----
* ~~add ability to update and download specific games or new-items only~~
* add 'clean' command to orphan/remove old or unexpected files to keep your collection clean with only the latest files
* support resuming manifest updating
* ~~add support for incremental manifest updating (ie. only fetch newly added games) rather than fetching entire collection information~~
* ability to customize/remap default game directory name
* add GOG movie support
* ... feel free to contact me with ideas or feature requests!
