import argparse


def with_cmdargs():

    root_parser = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(prog='ihttpd', description="ihttpd 命令行工具")
    subparsers = parser.add_subparsers(dest='command')

    init_parser = subparsers.add_parser('init', help='init', parents=[root_parser])
    init_parser.add_argument('--network', type=str, default="private", help='network')
    init_parser.add_argument('--bandwidth', type=int, default="100", help='bandwidth')
    init_parser.add_argument('--parallel', type=int, default="200", help='parallel')
    init_parser.set_defaults(func=init_with_cmdargs)

    cmd_args = parser.parse_args()
    if hasattr(cmd_args, 'func'):
        try:
            cmd_args.func(cmd_args)
        except Exception: # noqa
            pass
        except KeyboardInterrupt:
            print()
            pass
    else:
        parser.print_help()


def init_with_cmdargs(cmd_args):
    import pathlib

    try:
        from ..helper import figlet
        from .. import httpdrs


        figlet.print_figlet()

        use_path = pathlib.Path("").absolute().__str__()
        presign = "http://internal-data.baai.ac.cn/api/v1/storage/sign/download/presign"
        network = cmd_args.network
        bandwidth = cmd_args.bandwidth
        parallel = cmd_args.parallel

        print(f"ihttpd: use_path, {use_path}")
        print(f"ihttpd: presign, {presign}")
        print(f"ihttpd: network, {network}")
        print(f"ihttpd: bandwidth, {bandwidth}")
        print(f"ihttpd: parallel, {parallel}")

        httpdrs.multi_download(use_path, presign, network,bandwidth,  parallel)

        httpdrs.push("---start---")
        for meta_bin in (pathlib.Path("").absolute() / "meta").glob("*.bin"):
            httpdrs.push(meta_bin.name)
        httpdrs.push("---end---")

        httpdrs.wait()

    except Exception as e:
        print(e)
