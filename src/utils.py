def move_file(dbutils, source, destination):
    try:
        dbutils.fs.mv(source, destination, True)
    except Exception as e:
        print(f"Error moving file: {str(e)}")


def log(message):
    print(f"[LOG]: {message}")