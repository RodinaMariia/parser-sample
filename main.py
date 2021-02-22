from config import settings
from parsing import utils as util


if __name__ == '__main__':
    conditions = [{'ktruCodeNameList': ['21.10.60.191-00000054']}]
    print('start')
    util.parse_sites(util.parser.ContractParser,
                     input_directory=settings.input_directory,
                     output_directory=settings.output_directory)
