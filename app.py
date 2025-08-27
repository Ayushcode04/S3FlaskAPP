from flask import Flask, render_template, request, redirect, url_for, send_file, flash
import os
import io
from werkzeug.utils import secure_filename
from botocore.exceptions import ClientError

import s3_service
from s3_service import s3_client

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "supersecretkey")

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

# ---------------- Object & Folder Routes ---------------- #
@app.route("/bucket/<bucket_name>")
def view_bucket(bucket_name):
    prefix = request.args.get("prefix", "")
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    try:
        resp = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
        folders = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        objects = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"] != prefix]
        all_buckets = s3_service.list_buckets()
    except ClientError as e:
        flash(f"❌ Error listing objects: {e}", "danger")
        folders = []
        objects = []
        all_buckets = []

    return render_template(
        "objects.html",
        bucket=bucket_name,
        prefix=prefix,
        folders=folders,
        objects=objects,
        all_buckets=all_buckets
    )

@app.route("/bucket/<bucket_name>/upload", methods=["POST"])
def upload_file(bucket_name):
    prefix = request.form.get("prefix", "") or ""
    if prefix and not prefix.endswith("/"):
        prefix = prefix + "/"

    file = request.files.get("file")
    if not file or file.filename == "":
        flash("No file selected", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=prefix))

    filename = secure_filename(file.filename)
    if filename == "":
        flash("Invalid filename", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=prefix))

    key = f"{prefix}{filename}"
    try:
        s3_client.upload_fileobj(file.stream, bucket_name, key)
        flash(f"✅ Uploaded {filename}", "success")
    except ClientError as e:
        flash(f"❌ Upload failed: {e}", "danger")

    return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=prefix))


@app.route("/bucket/<bucket_name>/download/<path:key>")
def download_file(bucket_name, key):
    try:
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        data = obj["Body"].read()
        buf = io.BytesIO(data)
        buf.seek(0)
        filename = key.split("/")[-1] or "download"
        return send_file(buf, as_attachment=True, download_name=filename)
    except ClientError as e:
        flash(f"❌ Download failed: {e}", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name))

@app.route("/bucket/<bucket_name>/delete/<path:key>")
def delete_file(bucket_name, key):
    parent_prefix = "/".join(key.split("/")[:-1])
    redirect_prefix = parent_prefix + "/" if parent_prefix else ""
    try:
        s3_client.delete_object(Bucket=bucket_name, Key=key)
        flash(f"✅ Deleted {key}", "success")
    except ClientError as e:
        flash(f"❌ Delete failed: {e}", "danger")
    return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=redirect_prefix))

# ---------------- Folder create & delete ---------------- #
@app.route("/bucket/<bucket_name>/create-folder", methods=["POST"])
def create_folder(bucket_name):
    parent_prefix = request.form.get("prefix", "") or ""
    folder_name = request.form.get("folder_name", "").strip()
    if parent_prefix and not parent_prefix.endswith("/"):
        parent_prefix = parent_prefix + "/"
    if not folder_name:
        flash("Folder name required", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=parent_prefix))
    folder_name = folder_name.strip("/")
    key = f"{parent_prefix}{folder_name}/"
    try:
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=b"")
        flash(f"✅ Folder '{folder_name}' created", "success")
    except ClientError as e:
        flash(f"❌ Create folder failed: {e}", "danger")
    return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=parent_prefix))

@app.route("/bucket/<bucket_name>/delete-folder", methods=["POST"])
def delete_folder(bucket_name):
    folder_prefix = request.form.get("folder_prefix", "")
    if not folder_prefix:
        flash("Folder prefix required", "danger")
        return redirect(url_for("view_bucket", bucket_name=bucket_name))
    if not folder_prefix.endswith("/"):
        folder_prefix = folder_prefix + "/"
    parent_parts = folder_prefix.rstrip("/").split("/")[:-1]
    redirect_prefix = "/".join(parent_parts) + "/" if parent_parts else ""
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        objs_to_delete = []
        for page in paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix):
            for obj in page.get("Contents", []):
                objs_to_delete.append({"Key": obj["Key"]})
                if len(objs_to_delete) == 1000:
                    s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objs_to_delete})
                    objs_to_delete = []
        if objs_to_delete:
            s3_client.delete_objects(Bucket=bucket_name, Delete={"Objects": objs_to_delete})
        flash(f"✅ Folder deleted: {folder_prefix}", "success")
    except ClientError as e:
        flash(f"❌ Delete folder failed: {e}", "danger")
    return redirect(url_for("view_bucket", bucket_name=bucket_name, prefix=redirect_prefix))

# ---------------- Copy & Move Routes ---------------- #

@app.route("/copy-object", methods=["POST"])
def copy_object():
    src_bucket = request.form["src_bucket"]
    src_key = request.form["src_key"]
    dest_bucket = request.form["dest_bucket"]
    # new: optional destination key (path or full key). Can be blank.
    dest_key_input = request.form.get("dest_key", "").strip()

    # compute final destination key
    if dest_key_input == "":
        dest_key = src_key
    elif dest_key_input.endswith("/"):
        # put inside folder, keep original filename
        dest_key = dest_key_input + os.path.basename(src_key)
    else:
        # use exactly provided key (rename)
        dest_key = dest_key_input

    try:
        s3_client.copy_object(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dest_bucket,
            Key=dest_key
        )
        flash(f"✅ File copied to {dest_bucket}/{dest_key}", "success")
    except ClientError as e:
        flash(f"❌ Copy failed: {e}", "danger")

    return redirect(url_for("view_bucket", bucket_name=src_bucket))


@app.route("/move-object", methods=["POST"])
def move_object():
    src_bucket = request.form["src_bucket"]
    src_key = request.form["src_key"]
    dest_bucket = request.form["dest_bucket"]
    dest_key_input = request.form.get("dest_key", "").strip()

    if dest_key_input == "":
        dest_key = src_key
    elif dest_key_input.endswith("/"):
        dest_key = dest_key_input + os.path.basename(src_key)
    else:
        dest_key = dest_key_input

    try:
        # Copy first
        s3_client.copy_object(
            CopySource={"Bucket": src_bucket, "Key": src_key},
            Bucket=dest_bucket,
            Key=dest_key
        )
        # Then delete from source (if dest is same bucket & same key this is effectively a no-op)
        s3_client.delete_object(Bucket=src_bucket, Key=src_key)
        flash(f"✅ File moved to {dest_bucket}/{dest_key}", "success")
    except ClientError as e:
        flash(f"❌ Move failed: {e}", "danger")

    return redirect(url_for("view_bucket", bucket_name=src_bucket))

if __name__ == "__main__":
    app.run(debug=True)
