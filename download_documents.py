import os
import boto3
import zipfile
import tempfile
from tqdm import tqdm
from datetime import datetime
from django.conf import settings
from django.utils.timezone import make_aware
from django.core.management.base import BaseCommand

from apps.users.models import UserDocuments


class Command(BaseCommand):
    help = "Download ID cards and/or selfies from AWS S3 cloud on the Desktop. Supports folder, zip, or both versions. (default: folder)"

    def add_arguments(self, parser):
        parser.add_argument(
            "--type",
            type=str,
            choices=["id_card", "selfie", "both"],
            required=True,
            help="Type of document to download: ID card, selfie, or both. (necessary)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Limit the number of documents to download. (optional) (default: all)",
        )

        zip_group = parser.add_mutually_exclusive_group()
        zip_group.add_argument(
            "--zip", action="store_true", help="Download directly into a zip without saving files locally. (optional)"
        )
        zip_group.add_argument(
            "--zip-with-folder",
            action="store_true",
            help="Download into a folder and also zip it. (optional)",
        )

        parser.add_argument("--start-date", type=str, help="Start date in YYYY-MM-DD format. (optional)")
        parser.add_argument("--end-date", type=str, help="End date in YYYY-MM-DD format. (optional)")

    def get_unique_filename(self, zipf, filename):
        base, ext = os.path.splitext(filename)
        counter = 1
        unique_name = filename

        while unique_name in zipf.namelist():
            unique_name = f"{base}_{counter}{ext}"
            counter += 1

        return unique_name

    def get_unique_filepath(self, folder, filename):
        base, ext = os.path.splitext(filename)
        counter = 1
        unique_name = filename
        full_path = os.path.join(folder, unique_name)

        while os.path.exists(full_path):
            unique_name = f"{base}_{counter}{ext}"
            full_path = os.path.join(folder, unique_name)
            counter += 1

        return full_path

    def download_documents(self, *args, **options):
        doc_type = options["type"]
        limit = options["limit"]
        zip_file = options["zip"]
        zip_with_folder = options["zip_with_folder"]

        # Parse optional dates
        start_date = options["start_date"]
        end_date = options["end_date"]
        filters = {}

        try:
            if start_date:
                start_dt = make_aware(datetime.strptime(start_date, "%Y-%m-%d"))
                filters["created_date__gte"] = start_dt

            if end_date:
                end_dt = make_aware(datetime.strptime(end_date, "%Y-%m-%d"))
                filters["created_date__lte"] = end_dt
        except ValueError:
            self.stdout.write(self.style.ERROR("Invalid start date format. Use YYYY-MM-DD."))
            return

        types_to_process = ["id_card", "selfie"] if doc_type == "both" else [doc_type]

        # Setup boto3
        s3 = boto3.client(
            "s3",
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=getattr(settings, "AWS_S3_REGION_NAME", "us-east-1"),
        )
        bucket_name = settings.AWS_STORAGE_BUCKET_NAME

        for dtype in types_to_process:
            self.stdout.write(self.style.NOTICE(f"\nProcessing '{dtype}' documents..."))

            docs = UserDocuments.objects.filter(type=dtype, file__isnull=False, **filters).order_by("created_date")

            # Optional limit
            if limit:
                docs = docs[:limit]

            total_size = 0
            s3_keys = []
            for doc in docs:
                try:
                    head = s3.head_object(Bucket=bucket_name, Key=doc.file.name)
                    total_size += head["ContentLength"]
                    s3_keys.append((doc.file.name, os.path.basename(doc.file.name)))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"\nError reading size for {doc.file.name}: {e}"))

            if not s3_keys:
                self.stdout.write(self.style.WARNING(f"\nNo valid {dtype} files to download."))
                continue

            self.stdout.write(self.style.SUCCESS(f"\nDownload started of {len(s3_keys)} '{dtype}' file(s)."))
            self.stdout.write("")

            zip_path = os.path.expanduser(f"~/Desktop/{dtype}s.zip")
            destination = os.path.expanduser(f"~/Desktop/{dtype}s")
            os.makedirs(destination, exist_ok=True)

            with tqdm(total=total_size, unit="B", unit_scale=True, desc=f"{dtype.capitalize()}s", leave=False) as pbar:
                try:
                    # Optional zip
                    if zip_file:
                        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                            for s3_key, filename in s3_keys:
                                try:
                                    with tempfile.NamedTemporaryFile() as temp:
                                        s3.download_fileobj(bucket_name, s3_key, temp, Callback=pbar.update)
                                        temp.seek(0)
                                        unique_name = self.get_unique_filename(zipf, filename)
                                        zipf.writestr(unique_name, temp.read())
                                except Exception as e:
                                    self.stdout.write(self.style.ERROR(f"Failed to add {filename} to ZIP: {e}"))

                    else:
                        # Download to folder
                        for s3_key, filename in s3_keys:
                            local_path = self.get_unique_filepath(destination, filename)
                            try:
                                with open(local_path, "wb") as f:
                                    s3.download_fileobj(bucket_name, s3_key, f, Callback=pbar.update)
                            except Exception as e:
                                self.stdout.write(self.style.ERROR(f"Failed to download {filename}: {e}"))

                        # Optional zip with folder
                        if zip_with_folder:
                            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                                for root, _, files in os.walk(destination):
                                    for file in files:
                                        full_path = os.path.join(root, file)
                                        arcname = os.path.relpath(full_path, start=destination)
                                        zipf.write(full_path, arcname)
                finally:
                    pbar.close()

            self.stdout.write(self.style.SUCCESS(f"Download complete for '{dtype}'."))
            if zip_file:
                self.stdout.write(self.style.SUCCESS(f"\nZIP created at {zip_path}"))
            else:
                self.stdout.write(self.style.SUCCESS(f"\nFolder created at {destination}"))

    def handle(self, *args, **options):
        try:
            self.download_documents(*args, **options)
        except (KeyboardInterrupt, EOFError):
            self.stdout.write(
                self.style.WARNING("\nProcess interrupted by user (CTRL+C / CTRL+D). Exiting gracefully...")
            )
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\nAn error occurred: {e}"))
