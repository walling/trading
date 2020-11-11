from typing import List
import click
from .show import show
from .download import download
from .index import index


@click.group()
def cli():
    pass


cli.add_command(show)
cli.add_command(download)
cli.add_command(index)


def main(args: List[str] = [], prog_name="dataset"):
    cli(args=args, prog_name=prog_name, standalone_mode=True)
