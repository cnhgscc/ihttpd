import argparse

def with_cmdargs():

    root_parser = argparse.ArgumentParser(add_help=False)

    parser = argparse.ArgumentParser(prog='baai-flagdataset', description="baai-flagdataset 命令行工具: [bf]")
    subparsers = parser.add_subparsers(dest='command')

    init_parser = subparsers.add_parser('init', help='init', parents=[root_parser])
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

    try:
        from ..baai_helper import baai_print
        from ..baai_flagdataset_rs import multi_download


        baai_print.print_figlet()

        multi_download(
            "/Users/hgshicc/test/flagdataset/AIM-500",
            # "http://127.0.0.1:30000/v1/storage/download/presign",
            "http://internal-data.baai.ac.cn/api/v1/storage/sign/download/presign",
            "public",
            10
        )

    except Exception as e:
        print(e)

