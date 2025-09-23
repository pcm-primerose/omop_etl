import polars as pl
from pathlib import Path
from omop_etl.harmonization.datamodels import HarmonizedData
from omop_etl.harmonization.harmonizers.impress import ImpressHarmonizer

# todo: implement cli later
# app = typer.Typer(add_completion=True)
#
# log = getLogger(__name__)
#
#
# def process_impress(file: Path) -> HarmonizedData:
#     df = pl.read_csv(file)
#     return ImpressHarmonizer(df, trial_id="IMPRESS").process()
#
#
# def process_drup(file: Path) -> HarmonizedData:
#     raise NotImplementedError
#
#
# @app.command()
# def impress(file: Path):
#     _ = process_impress(file)
#     typer.echo(f"Harmonized {file}")
#
#
# def main():
#     app()
#
#


def drup_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def impress_data(file: Path) -> pl.DataFrame:
    data = pl.read_csv(file)
    return data


def process_impress(file: Path) -> HarmonizedData:
    data = impress_data(file)
    harmonizer = ImpressHarmonizer(data, trial_id="IMPRESS")
    return harmonizer.process()


# def process_drup(file: Path) -> HarmonizedData:
#     data = drup_data(file)
#     harmonizer = DrupHarmonizer(data=data, trial_id="DRUP")
#     return harmonizer.process()


if __name__ == "__main__":
    impress_150_file = Path(__file__).parents[3] / ".data" / "preprocessing" / "impress" / "20250909T144845Z_19d79919" / "data_preprocessed.csv"
    impress_1k_file = Path(__file__).parents[3] / ".data" / "preprocessing" / "impress" / "20250909T144845Z_19d79919" / "data_preprocessed.csv"

    impress = process_impress(impress_150_file)
    print("\n")
    # drup = process_drup(drup_file)
