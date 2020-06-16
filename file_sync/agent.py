import pkg_resources


def main():
    print("Loading file sync plugins ..")
    for entry_point in pkg_resources.iter_entry_points("file_sync_plugins"):
        print(f"Found plugin: {entry_point}")
        file_sync_plugin = entry_point.load()
        print(f"Loaded {entry_point.name} plugin.")
        file_sync_plugin()


if __name__ == "__main__":
    main()
