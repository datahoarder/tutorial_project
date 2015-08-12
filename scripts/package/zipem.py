# a script to zip up a file
from zipfile import ZipFile


def zip_files(dest, files):
    with ZipFile(dest, 'w') as z:
        for f in files:
            z.write(f)
    # let the context manager close the file
    # maybe return something else besides the path...?
    return dest

