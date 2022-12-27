

def reader_factory(path):

    protocols = {
        "gcs": "google",
        "s3": "amazon",
        "file": "disk",
        "az": "azure"
    }

    protocol = path.split("://")[0]
    return protocols.get(protocol, "disk")


def just_a_test(path):
    reader_class = reader_factory(path)
    reader = reader_class()
    data = reader.read(path)