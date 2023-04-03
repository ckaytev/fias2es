import argparse

from fias2es import fias_parser, upload_elastic

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--region_id')
    args = parser.parse_args()

    upload_elastic.upload(
        fias_parser.parser(region_id=args.region_id)
        )