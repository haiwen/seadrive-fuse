# seadrive-fuse
SeaDrive daemon with FUSE interface

# Building
## Ubuntu Linux
```
sudo apt-get install autoconf automake libtool libevent-dev libcurl4-openssl-dev libgtk2.0-dev uuid-dev intltool libsqlite3-dev valac libjansson-dev libssl-dev
```

First, you shoud get the latest source of [libsearpc](https://github.com/haiwen/libsearpc) with `v3.2-latest` tag and [seadrive-fuse](https://github.com/haiwen/seadrive-fuse).

To build [seadrive-fuse](https://github.com/haiwen/seadrive-fuse), you need first build [libsearpc](https://github.com/haiwen/libsearpc).
### libsearpc
```
git clone --branch=v3.2-latest https://github.com/haiwen/libsearpc.git
cd libsearpc
./autogen.sh
./configure
make
sudo make install
```
### seadrive-fuse
```
git clone https://github.com/haiwen/seadrive-fuse.git
cd seadrive-fuse
./autogen.sh
./configure
make
sudo make install
```

**Note:** If you plan to package for distribution, you should compile with the latest tag instead of the master branch.
