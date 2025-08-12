# call pre-process here ...
def output_dir_preprocessing() -> Path:
    outpath = Path(__file__).parents[1] / ".data" / "preprocessing"
    if not outpath.exists():
        outpath.mkdir(parents=True, exist_ok=True)

    return outpath


def raw_impress_data() -> Path:
    return Path(__file__).parents[2] / "ecrf_mocker" / "output"


def config_path() -> Path:
    return Path(__file__).parents[1] / "configs" / "impress_ecrf_variables.json"


def main():
    # pre-process synthetic data to one file
    pre_processed_data: Path = impress_preprocessor(
        input_path=raw_impress_data(),
        output_path=output_dir_preprocessing() / "preprocessed_impress_synthetic.csv",
        config_path=config_path(),
    )
