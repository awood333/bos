'''rpipeline/aws/s3_loader.py'''

import subprocess

class S3Utils:

    @staticmethod
    def s3_upload_png(prefix, filename, png_bytes, bucket='cow-bucket-613211402323-ap-southeast-7-an',
                       remote_name='AWS_S3', rclone_path='rclone'):
        remote_target = f"{remote_name}:{bucket}/{prefix}/{filename}"
        try:
            result = subprocess.run(
                [rclone_path, "rcat", remote_target, "--s3-no-check-bucket"],
                input=png_bytes,
                check=True,
                capture_output=True,
            )
            print(f"Successfully uploaded {filename} to {remote_target}")
        except subprocess.CalledProcessError as e:
            print(f"Failed to upload {filename} to {remote_target}")
            print(f"rclone error: {e.stderr}")


# module-level function so `from utilities.s3_loader import s3_upload_png` works as written in plot_net_revenue_model.py
def s3_upload_png(prefix, filename, png_bytes, **kwargs):
    S3Utils.s3_upload_png(prefix, filename, png_bytes, **kwargs)