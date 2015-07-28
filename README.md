# GBD

This is a python program that can use your google drive's space to create a virtual block device.
Unlike gdrive or other projects, it doesn't provide a google-drive-integrated file system. Instead, it's just a block device that you may format into any format you like.

GBD is not yet stable and may cause issues (freezing your X window, etc), I'll be glad if you can send me a bug report (with debug output, plz) or even a patch.

## Structure

GBD is splitted into back-end and front-end.
The back-end is the part that manipulates your google drive and the front-end is the part that warps back-end into a block device.

### Back-end

* `gbd.py` is the core of GBD, it is capable of communicating with google drive and exports a block-based I/O interface.
* `cached_gbd.py` is build upon `gbd.py`. It provides cache and a friendly (compares to `gbd.py`) I/O interface.

### Front-end

* `nbd.py` is the only front-end right now. It warps `cached_gbd.py` into a nbd server and you may connect it to a nbd device via nbd-client.

## How to

Run `nbd.py` to launch the nbd server. Since GBD utilize OAuth, it will ask you to visit a link to get the code. You may just follow the instruction.

```
$ ./nbd.py
```

After the server starts, you'll use nbd-client to connect to our server. Remember to replace *$WHATEVER_NAME_YOU_LIKE* into whatever name your like.

```
$ sudo nbd-client $WHATEVER_NAME_YOU_LIKE localhost 10809 /dev/nbd0
```

If this is the first time, `nbd.py` will ask you to enter desired block size / total size / cache size. You may use 64K/1G/128M if you're just trying it.

Your block device should be ready now, let's try to do something.

```
$ sudo mke2fs /dev/nbd0
$ sudo mount /dev/nbd0 /mnt
$ ls /mnt
```

Yeah, have a good time!

## Bugs

* May use trashed directory on google drive
* May freeze your X window
