import pytest

from edp.common.config import Settings
from edp.common.paths import LakeLayout, Layer


def test_local_path_is_under_lake_root(tmp_path):
    layout = LakeLayout(Settings(_env_file=None, local_lake_root=str(tmp_path)))
    assert layout.table_path(Layer.SILVER, "orders") == f"{tmp_path.as_posix()}/silver/orders"


def test_relative_lake_root_resolves_against_project_root():
    path = LakeLayout(Settings(_env_file=None, local_lake_root="data/lake")).root
    assert path.endswith("/data/lake")
    assert not path.startswith("data")


def test_s3_path_uses_s3a_scheme_and_prefix():
    s = Settings(_env_file=None, storage_backend="s3", s3_bucket="my-bkt", s3_prefix="/edp/")
    assert LakeLayout(s).table_path(Layer.BRONZE, "customers") == (
        "s3a://my-bkt/edp/bronze/customers"
    )


@pytest.mark.parametrize("bad", ["", "a/b"])
def test_invalid_table_names_rejected(bad):
    with pytest.raises(ValueError):
        LakeLayout(Settings(_env_file=None)).table_path(Layer.GOLD, bad)
