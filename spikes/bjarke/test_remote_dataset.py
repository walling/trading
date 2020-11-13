import pyarrow.dataset as ds
import fsspec
import logging

logging.basicConfig(level="DEBUG")

url = fsspec.open("https://bjarke.me/s/dataset_ygwwjag5qcb8odisldw1kooszvym3qer/trades")


def create_proxy(self, key):
    def proxy(*args, **kwargs):
        # print(key, args, kwargs);
        result = getattr(self._fs, key)(*args, **kwargs)
        # print('=>', result)
        return result

    return proxy


class FSAdapter(fsspec.spec.AbstractFileSystem):
    def __init__(self, fs):
        self._fs = fs
        print(self.__dict__)
        for key, value in fs.__dict__.items():
            if callable(value):
                if key in ["info", "isfile"]:  # key in self.__dict__
                    print("not proxying:", key)
                else:
                    print("proxying:", key)
                    setattr(self, key, create_proxy(self, key))
            else:
                print("setting:", key)
                setattr(self, key, value)

    def info(self, path):
        result = self._fs.info(path)
        if result["type"] == "file" and not path.endswith(".parquet"):
            result["type"] = "directory"
        return result

    def isfile(self, path):
        return path.endswith(".parquet")


fs = FSAdapter(url.fs)
dataset = ds.dataset(url.path, filesystem=fs, format="parquet", partitioning="hive")
print(dataset)
