
import argparse
import logging

from variant import reader


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("-d", help="Scan directory for new data")
    parser.add_argument("-a", help="Archive folder for processed data")
    args = parser.parse_args()

    if args.d is None or args.a is None:
        parser.print_help()
        return

    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s', filename="lazarus.log", level=10)
    logging._srcfile = None
    logging.logProcesses = 0

    try:
        reader(args.d, args.a)
    except Exception as e:
        logging.critical(e)


if __name__  == "__main__":
    main()

