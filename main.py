from config import settings
from parsing import utils as util


def start_with(todo: str, conditions=None):
    if todo == 'url':
        # Create list with url
        util.create_urls(urls_directory=settings.input_directory,
                         conditions=conditions,
                         use_slicing=True, slow=False)
    else:
        # Parse sites from prepared list
        util.parse_sites(util.parser.ContractParser, settings, storage_type='sql')


if __name__ == '__main__':
    start_with('parse')
  
