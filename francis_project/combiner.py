import argparse
import csv
import itertools
from pathlib import Path
from pyxtension.streams import stream
import shutil
from PIL import Image
import imagehash
import HeifImagePlugin
import re
import os

def main():
    # Initialize argument parser.
    parser = argparse.ArgumentParser(description='Merger and deduplicator of images.')

    # Arguments regarding watermarking
    parser.add_argument('-s', '--SourceDir', type=str,
                        help='Source Directory.', required=True)

    parser.add_argument('-o', '--OutputDir', type=str,
                        help='Output Directory.', required=True)

    parser.add_argument('-e', '--Execute', required=False, default=False)

    args = vars(parser.parse_args())

    # Extract args
    source_dir = Path(args['SourceDir'])
    output_dir = Path(args['OutputDir'])
    should_execute = args['Execute']
    should_execute = True

    # Determine for certain that a photo is duplicate if both phash and name collide
    all_images = [file for file in source_dir.glob("**/*") if is_image(file)]
    img_dir = {k: k.name.lower() for k in all_images}
    # Generate the PHashes for all the photos
    grouped_phash = generate_phashes(all_images)
    images_grouped_name = stream(img_dir.items).groupBy(lambda item: item[1]).toMap()

    for (hash, tups) in grouped_phash.items():
        imgs = [x[1] for x in tups]
        # In case of unique image, move to output folder
        if len(imgs) == 1:
            img_path: Path = imgs[0]
            output_file = output_dir.joinpath(img_path.name)
            if not output_file.exists():
                output_file.parent.mkdir(parents=True, exist_ok=True)
                do_copy(img_path, output_file, execute=should_execute)
            else:
                print('%s already exists, skipping...' % img_path)
        # In case duplicates are found, move them in respective folders
        else:
            # First make directory for the files if not yet exists
            output_files_dir = output_dir.joinpath("%s-%s" % (imgs[0].name.lower(), hash))
            output_files_dir.mkdir(parents=True, exist_ok=True)
            # Copy duplicate images to the folder
            for (index, img) in enumerate(imgs):

                output_file = output_files_dir.joinpath(Path("%s_%d%s" % (img.stem, index, img.suffix)))
                if not output_file.exists():
                    do_copy(img, output_file, execute=should_execute)
                else:
                    print('%s already exists, skipping...' % img)

def do_copy(src, dest, execute=False):
    if execute is True:
        shutil.copy2(src, dest)

def is_image(file: Path):
    suffix = file.suffix.__str__()
    match = re.compile(r".?.heic|png|jpg|jpeg|tiff|tif|raw|eps|gif|bmp", flags=re.I).findall(suffix)
    return len(match) != 0

def generate_phashes(all_images) :
    hashes_file = Path(".").joinpath("hashes.csv")
    hashes_file.touch(exist_ok=True)

    # Read all the currently hashed files
    hashed_files = []
    hashes = []
    with open(hashes_file, 'r', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        for entry in reader:
            hashed_files.append(entry[1])
            hashes.append((entry[0], Path('.').joinpath(entry[1])))

    # Calculate the difference between the already existing images and the new structure
    path_str_dict = {path.__str__(): path for path in all_images}
    unhashed_str = list(set([x.__str__() for x in all_images]) - set(hashed_files))
    unhashed_paths = [path_str_dict[x] for x in unhashed_str]

    # Add missing ones
    with open(hashes_file, 'a', newline='') as csvfile:
        for img in unhashed_paths:
            parent = img.parent
            stem = img.stem
            match = [x for x in parent.glob("%s__h_*" % stem)]
            if len(match) == 0:
                hash = imagehash.phash(Image.open(img))
                out_file = parent.joinpath("%s__h_%s" % (stem, hash))
                out_file.touch()
            else:
                hash = re.compile(r".+?__h_(.+)").match(match[0].name).group(1)

            hashes.append([hash, img])

            writer = csv.writer(csvfile, delimiter=',')
            writer.writerow([hash, img])

    grouped = stream(hashes).groupBy(lambda tup: tup[0]).toMap()
    return grouped

# Calls main function when script is run.
if __name__ == "__main__":
    main()




# # Group images based on filename
    # images_grouped_name = stream(img_dir.items).groupBy(lambda item: item[1]).toMap()
    # images_grouped_hash = stream(img_dir.items).groupBy(lambda item: imagehash.phash(Image.open(item[0]))).toMap()
    #
    # for (img_name, images) in images_grouped_name:
    #     # images = [x[0] for x in grouped_images]
    #
    #     # In case of unique image, move to output folder
    #     if len(images) == 1:
    #         img_path: Path = images[0][0]
    #         output_file = output_dir.joinpath(img_path.name)
    #         if not output_file.exists():
    #             output_file.parent.mkdir(parents=True, exist_ok=True)
    #             do_copy(img_path, output_file, execute=should_execute)
    #         else:
    #             print('%s already exists, skipping...' % img_path)
    #     # In case duplicates are found, move them in respective folders
    #     else:
    #         # First make directory for the files if not yet exists
    #         output_files_dir = output_dir.joinpath(img_name)
    #         output_files_dir.mkdir(parents=True, exist_ok=True)
    #         # Copy duplicate images to the folder
    #         for (index, img) in enumerate([x[0] for x in images]):
    #
    #             output_file = output_files_dir.joinpath(Path("%s_%d%s" % (img.stem, index, img.suffix)))
    #             if not output_file.exists():
    #                 do_copy(img, output_file, execute=should_execute)
    #             else:
    #                 print('%s already exists, skipping...' % img)