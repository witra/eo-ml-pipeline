from eo_pipeline.utils.path import delete_path

def test_delete_path(tmp_path):
    """Delete files and directories."""

    file = tmp_path / "test.txt"
    file.write_text("test")

    directory = tmp_path / "test_dir"
    directory.mkdir()

    delete_path(file)
    delete_path(directory)

    assert not file.exists()
    assert not directory.exists()