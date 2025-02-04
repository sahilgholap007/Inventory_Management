from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import os
from datetime import datetime, timedelta
from database import get_db_connection

app = Flask(__name__)
CORS(app)

UPLOAD_FOLDER = "uploads"
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# Define required columns
REQUIRED_COLUMNS = {
    "order_id", "awb", "status", "order_date", "marketplace_name", "product_name",
    "selling_price", "shipping_date", "marked_date"
}

@app.route("/upload", methods=["POST"])
def upload_excel():
    if "files" not in request.files:
        return jsonify({"message": "No files uploaded"}), 400

    uploaded_files = request.files.getlist("files")
    dataframes = []

    for file in uploaded_files:
        filepath = os.path.join(UPLOAD_FOLDER, file.filename)
        file.save(filepath)

        # Read Excel file
        df = pd.read_excel(filepath, dtype=str)

        # Standardize column names
        df.columns = df.columns.str.strip().str.lower()

        # Keep only the required columns
        df = df[[col for col in REQUIRED_COLUMNS if col in df.columns]]

        # Fill missing columns with None
        for col in REQUIRED_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Replace NaN values with None for MySQL compatibility
        df = df.where(pd.notna(df), None)

        # Convert date columns to proper format
        for date_col in ["order_date", "shipping_date", "marked_date"]:
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date  # Convert to date format

        # Append to list
        dataframes.append(df)

    # Combine all DataFrames
    if not dataframes:
        return jsonify({"message": "No valid data found in uploaded files"}), 400

    combined_df = pd.concat(dataframes, ignore_index=True)

    # Insert into database
    conn = get_db_connection()
    cursor = conn.cursor()

    for _, row in combined_df.iterrows():
        order_id = row["order_id"]
        awb = row["awb"]
        status = row["status"]
        order_date = row["order_date"]
        marketplace_name = row["marketplace_name"]
        product_name = row["product_name"]
        selling_price = row["selling_price"]
        shipping_date = row["shipping_date"]
        marked_date = row["marked_date"]

        # Ensure `marked_date` remains blank initially
        if marked_date is None:
            marked_date = None  # Store as NULL in MySQL

        # Check if the same order_id and awb already exist in DB
        cursor.execute(
            "SELECT marked_date FROM orders WHERE order_id = %s AND awb = %s",
            (order_id, awb)
        )
        existing_record = cursor.fetchone()

        # If the order_id & awb already exist, update marked_date with the current date
        if existing_record:
            marked_date = datetime.today().date()  # Update marked_date to today

        # Check if order status is "shipped" for more than one month without being "delivered" or "RTO"
        if status == "shipped" and shipping_date:
            shipping_date_obj = datetime.strptime(str(shipping_date), "%Y-%m-%d").date()
            if (datetime.today().date() - shipping_date_obj) > timedelta(days=30):  # More than 1 month
                status = "Lost/Undelivered"

        cursor.execute(
            """
            INSERT INTO orders (order_id, awb, status, order_date, marketplace_name, product_name, 
                               selling_price, shipping_date, marked_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE status=%s, marked_date=%s
            """,
            (order_id, awb, status, order_date, marketplace_name, product_name, 
             selling_price, shipping_date, marked_date, 
             status, marked_date)
        )

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"message": "Files processed successfully", "records_inserted": len(combined_df)}), 200


@app.route("/orders", methods=["GET"])
def get_orders():
    marketplace = request.args.get("marketplace")
    status = request.args.get("status")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = """SELECT order_id, awb, status, order_date, marketplace_name, product_name, selling_price, shipping_date, marked_date
               FROM orders WHERE 1=1"""
    params = []

    if marketplace:
        query += " AND marketplace_name=%s"
        params.append(marketplace)
    if status:
        query += " AND status=%s"
        params.append(status)
    if start_date and end_date:
        query += " AND order_date BETWEEN %s AND %s"
        params.append(start_date)
        params.append(end_date)

    cursor.execute(query, params)
    results = cursor.fetchall()

    cursor.close()
    conn.close()

    return jsonify(results)

@app.route("/download", methods=["GET"])
def download_excel():
    marketplace = request.args.get("marketplace")
    status = request.args.get("status")
    start_date = request.args.get("start_date")
    end_date = request.args.get("end_date")

    conn = get_db_connection()
    cursor = conn.cursor()

    query = """SELECT order_id, awb, status, order_date, marketplace_name, product_name, 
                      selling_price, shipping_date, marked_date
               FROM orders WHERE 1=1"""
    params = []

    if marketplace:
        query += " AND marketplace_name=%s"
        params.append(marketplace)
    if status:
        query += " AND status=%s"
        params.append(status)
    if start_date and end_date:
        query += " AND order_date BETWEEN %s AND %s"
        params.append(start_date)
        params.append(end_date)

    cursor.execute(query, params)
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]

    df = pd.DataFrame(rows, columns=columns)

    # Format dates properly before exporting
    for date_col in ["order_date", "status_update_date", "shipping_date", "marked_date"]:
        if date_col in df.columns:
            df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.strftime("%Y-%m-%d")

    filepath = os.path.join(UPLOAD_FOLDER, "orders_export.xlsx")
    df.to_excel(filepath, index=False)

    cursor.close()
    conn.close()

    return send_file(filepath, as_attachment=True)

@app.route("/download_template", methods=["GET"])
def download_template():
    # Define required columns
    template_columns = [
        "order_id", "awb", "status", "order_date", "marketplace_name", "product_name",
        "selling_price", "shipping_date", "marked_date"
    ]

    # Create an empty DataFrame with required columns
    df = pd.DataFrame(columns=template_columns)

    # Add example row for reference
    example_data = {
        "order_id": "123456",
        "awb": "AWB987654",
        "status": "shipped",
        "order_date": "2024-01-10",
        "marketplace_name": "Amazon",
        "product_name": "Smartphone",
        "selling_price": "499",
        "shipping_date": "2024-01-11",
        "marked_date": "2024-01-15"
    }
    
    df = df._append(example_data, ignore_index=True)

    # Save the template as an Excel file
    template_path = os.path.join(UPLOAD_FOLDER, "orders_template.xlsx")
    df.to_excel(template_path, index=False)

    return send_file(template_path, as_attachment=True)

@app.route("/update_status", methods=["POST"])
def update_order_status():
    """Updates order status based on an uploaded file with 'order_id' and 'awb'."""
    if "file" not in request.files or "status" not in request.form:
        return jsonify({"message": "File and status are required"}), 400

    file = request.files["file"]
    selected_status = request.form["status"]  # The new status (e.g., "RTO", "Delivered")

    if selected_status not in ["RTO", "Delivered"]:
        return jsonify({"message": "Invalid status selected"}), 400

    filepath = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(filepath)

    df = pd.read_excel(filepath, dtype=str)
    df.columns = df.columns.str.strip().str.lower()

    if not {"order_id", "awb"}.issubset(df.columns):
        return jsonify({"message": "File must contain 'order_id' and 'awb' columns"}), 400

    conn = get_db_connection()
    cursor = conn.cursor()

    # Update status for each order_id and awb pair
    for _, row in df.iterrows():
        cursor.execute(
            """
            UPDATE orders 
            SET status=%s 
            WHERE order_id=%s AND awb=%s
            """,
            (selected_status, row["order_id"], row["awb"])
        )

    conn.commit()
    cursor.close()
    conn.close()

    return jsonify({"message": f"Orders updated to '{selected_status}' successfully", "records_updated": len(df)}), 200


def handle_status_mismatch(your_file_df, delivery_partner_df):
    """
    Compare statuses between two DataFrames and identify mismatches.
    If a mismatch is found (e.g., your file has 'RTO' but the partner file has 'Delivered'),
    mark it as 'Status Mismatch' in the database.
    """

    # Standardize column names
    your_file_df.columns = your_file_df.columns.str.strip().str.lower()
    delivery_partner_df.columns = delivery_partner_df.columns.str.strip().str.lower()

    # Ensure both files have required columns
    required_columns = {"order_id", "awb", "status"}
    if not required_columns.issubset(your_file_df.columns) or not required_columns.issubset(delivery_partner_df.columns):
        return {"message": "Both files must contain 'order_id', 'awb', and 'status' columns"}, 400

    # Merge both DataFrames on order_id and awb
    merged_df = pd.merge(your_file_df, delivery_partner_df, on=["order_id", "awb"], suffixes=("_your", "_partner"))

    # Identify mismatches
    mismatched_df = merged_df[merged_df["status_your"] != merged_df["status_partner"]]

    if mismatched_df.empty:
        return {"message": "No status mismatches found"}, 200

    # Update database with 'Status Mismatch' where there is a conflict
    conn = get_db_connection()
    cursor = conn.cursor()

    for _, row in mismatched_df.iterrows():
        cursor.execute(
            """
            UPDATE orders 
            SET status = 'Status Mismatch' 
            WHERE order_id = %s AND awb = %s
            """,
            (row["order_id"], row["awb"])
        )

    conn.commit()
    cursor.close()
    conn.close()

    return {"message": f"Status mismatches detected and updated for {len(mismatched_df)} records"}, 200


@app.route("/compare_status", methods=["POST"])
def compare_status():
    """API to compare statuses between the user's file and the delivery partner's file."""
    if "your_file" not in request.files or "partner_file" not in request.files:
        return jsonify({"message": "Both files are required"}), 400

    your_file = request.files["your_file"]
    partner_file = request.files["partner_file"]

    # Save uploaded files temporarily
    your_file_path = os.path.join(UPLOAD_FOLDER, your_file.filename)
    partner_file_path = os.path.join(UPLOAD_FOLDER, partner_file.filename)
    your_file.save(your_file_path)
    partner_file.save(partner_file_path)

    # Read the files as DataFrames
    your_file_df = pd.read_excel(your_file_path, dtype=str)
    partner_file_df = pd.read_excel(partner_file_path, dtype=str)

    # Process status mismatch detection
    response, status_code = handle_status_mismatch(your_file_df, partner_file_df)

    return jsonify(response), status_code

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=8000)
