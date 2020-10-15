from pysearpc import searpc_func, SearpcError, NamedPipeClient


class SeadriveRpcClient(NamedPipeClient):
    """RPC used in client"""

    def __init__(self, socket_path, *args, **kwargs):
         NamedPipeClient.__init__(
             self,
             socket_path,
             "seadrive-rpcserver",
             *args,
             **kwargs
         )

    @searpc_func("int", [])
    def seafile_enable_auto_sync():
        pass
    enable_auto_sync = seafile_enable_auto_sync

    @searpc_func("int", [])
    def seafile_disable_auto_sync():
        pass
    disable_auto_sync = seafile_disable_auto_sync

    @searpc_func("json", [])
    def seafile_get_global_sync_status():
        pass
    get_global_sync_status = seafile_get_global_sync_status

