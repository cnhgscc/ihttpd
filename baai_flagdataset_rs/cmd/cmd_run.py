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
        from ..baai_flagdataset_rs import run_flagdataset


        baai_print.print_figlet()

        run_flagdataset()

        print("flagdataset 初始化完成")
    except Exception as e:
        print(e)

