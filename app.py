from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import s3_service
import os
from s3_service import s3_client
from botocore.exceptions import ClientError
from werkzeug.utils import secure_filename
import io

app = Flask(__name__)
app.secret_key = "supersecretkey"  # Keep secure in .env for production

@app.route("/")
def home():
    return redirect("/buckets")

# ---------------- Bucket Routes ---------------- #
@app.route("/buckets")
def list_buckets_page():
    buckets = s3_service.list_buckets()
    return render_template("buckets.html", buckets=buckets)

@app.route("/create-bucket", methods=["POST"])
def create_bucket():
    bucket_name = request.form["bucket_name"].strip()
    try:
        s3_service.create_bucket(bucket_name)
        flash(f"✅ Bucket '{bucket_name}' created", "success")
    except Exception as e:
        flash(f"❌ Create bucket failed: {e}", "danger")
    return redirect("/buckets")

@app.route("/delete-bucket", methods=["POST"])
def delete_bucket():
    bucket_name = request.form["bucket_name"].strip()
    try:
        s3_service.delete_bucket(bucket_name)
        flash(f"✅ Bucket '{bucket_name}' deleted", "success")
    except Exception as e:
        flash(f"❌ Delete bucket failed: {e}", "danger")
    return redirect("/buckets")

# ---------------- Object Routes ---------------- #
@app.route("/bucket/<bucket_name>")
def view_bucket(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        objects = [obj["Key"] for obj in response.get("Contents", [])]
        all_buckets = s3_service.list_buckets()
    except ClientError as e:
        flash(f"❌ Error listing objects: {e}", "danger")
        objects = []
        all_buckets = []
    return render_template(
        "objects.html",
        bucket=bucket_name,
        objects=objects,
        all_buckets=all_buckets
    )

@app.route("/bucket/<bucket_name>/upload", methods=["POST"])
def upload_file(bucket_name):
    file = request.files.get("file")
    if not file or file.filename == "":
        flash("No file selected", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name))

    filename = secure_filename(file.filename)
    if filename == "":
        flash("Invalid filename", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name))

    try:
        # use file.stream for upload_fileobj
        s3_client.upload_fileobj(file.stream, bucket_name, filename)
        flash(f"✅ Uploaded {filename}", "success")
    except ClientError as e:
        flash(f"❌ Upload failed: {e}", "danger")

    return redirect(url_for("view_bucket", bucket_name=bucket_name))

# NOTE: use <path:key> so keys that contain / work
@app.route("/bucket/<bucket_name>/download/<path:key>")
def download_file(bucket_name, key):
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        data = obj["Body"].read()
        buf = io.BytesIO(data)
        buf.seek(0)
        # derive a safe filename from key
        filename = key.split("/")[-1] or "download"
        return send_file(buf, as_attachment=True, download_name=filename)
    except ClientError as e:
        flash(f"❌ Download failed: {e}", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name))

@app.route("/bucket/<bucket_name>/delete/<path:key>")
def delete_file(bucket_name, key):
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        flash(f"✅ Deleted {key}", "success")
    except ClientError as e:
        flash(f"❌ Delete failed: {e}", "danger")
    return redirect(url_for("view_bucket", bucket_name=bucket_name))

# ---------------- Copy & Move Routes ---------------- #
@app.route("/copy-object", methods=["POST"])
def copy_object():
    src_bucket = request.form["src_bucket"]
    src_key = request.form["src_key"]
    dest_bucket = request.form["dest_bucket"]

    try:
        s3_client.copy_object(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dest_bucket,
            Key=src_key
        )
        flash(f"✅ File copied to {dest_bucket}", "success")
    except ClientError as e:
        flash(f"❌ Copy failed: {e}", "danger")

    return redirect(url_for("view_bucket", bucket_name=src_bucket))


@app.route("/move-object", methods=["POST"])
def move_object():
    src_bucket = request.form["src_bucket"]
    src_key = request.form["src_key"]
    dest_bucket = request.form["dest_bucket"]

    try:
        # Copy first
        s3_client.copy_object(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dest_bucket,
            Key=src_key
        )
        # Then delete
        s3_client.delete_object(Bucket=src_bucket, Key=src_key)
        flash(f"✅ File moved to {dest_bucket}", "success")
    except ClientError as e:
        flash(f"❌ Move failed: {e}", "danger")

    return redirect(url_for("view_bucket", bucket_name=src_bucket))


if __name__ == "__main__":
    app.run(debug=True)
