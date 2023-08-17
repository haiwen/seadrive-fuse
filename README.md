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
To build [seadrive-fuse](https://github.com/haiwen/seadrive-fuse), you need build [libwebsockets](https://github.com/warmcat/libwebsockets).
### libwebsockets
The default version of requires libwebsockets to be at least 4.0.20.
If the built-in version of your distribution is lower than the required version, you can choose to disable this function by set `--enable-ws` to no.
Also you can build a higher version of libwebsockets by yourself.

```
git clone --branch=v4.3.0 https://github.com/warmcat/libwebsockets
cd libwebsockets
mkdir build
cd build
cmake ..
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

**Note:** If you plan to package for distribution, you should compile with the latest tag instead of the master branch. Sometimes the latest tag for seadrive-gui project (https://github.com/haiwen/seadrive-gui) is higher than the tag here. In such case you should follow the tag in this project. For example, the latest tag for seadrive-fuse is v2.0.6 while for seadrive-gui is v2.0.7. You should build the package based on v2.0.6 tag from seadrive-gui too. This is because sometimes there is no update related to Linux in the new versions so this project is not updated.
