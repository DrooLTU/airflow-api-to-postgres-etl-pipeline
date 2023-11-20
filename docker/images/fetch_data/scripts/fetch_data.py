#!/usr/bin/env python
import click
import kaggle

@click.command()
@click.option('--dataset_name', help='Name of the Kaggle dataset to download')
@click.option('--output_path', default='.', help='Path to the output directory')
def download_kaggle_dataset(dataset_name, output_path):
    """
    Download files from a Kaggle dataset.

    Args:
        dataset_name (str): Name of the Kaggle dataset.
        output_path (str): Path to the output directory.
    """
    kaggle.api.dataset_download_files(dataset_name, path=output_path, unzip=True)
    click.echo(f"Downloaded files from {dataset_name} to {output_path}")

if __name__ == '__main__':
    download_kaggle_dataset()
