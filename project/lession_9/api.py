from flask import Flask, jsonify, request, Response, render_template
import pandas as pd
import io
import csv

app = Flask(__name__, template_folder='template', static_folder='static')

# Đọc dữ liệu
df = pd.read_csv('/Users/thangla/Documents/MindX_PY4E06/Data/sales_detail.csv')

# Tính doanh thu: doanh_thu = UnitPrice * OrderQty
df['doanh_thu'] = df['UnitPrice'] * df['OrderQty']


# =============================================
# Trang chủ - Dashboard HTML
# URL: /
# =============================================
@app.route('/')
def index():
    return render_template('interface.html')


# =============================================
# API: Lấy danh sách tất cả ProductID
# URL: /api/products
# =============================================
@app.route('/api/products')
def get_products():
    products = sorted(df['ProductID'].unique().tolist())
    return jsonify({"products": products})


# =============================================
# API: Tổng doanh thu theo từng sản phẩm (có filter)
# URL: /api/doanh-thu-san-pham
# Query params (tùy chọn):
#   - ids: danh sách ProductID cách nhau bởi dấu phẩy
#     Ví dụ: ?ids=776,777,778
# =============================================
@app.route('/api/doanh-thu-san-pham')
def doanh_thu_san_pham():
    ids_param = request.args.get('ids')

    du_lieu = df.copy()

    if ids_param:
        try:
            id_list = [int(x.strip()) for x in ids_param.split(',') if x.strip()]
            du_lieu = du_lieu[du_lieu['ProductID'].isin(id_list)]
        except ValueError:
            return jsonify({"loi": "ids phải là danh sách số nguyên, ví dụ: ?ids=776,777"}), 400

    if du_lieu.empty:
        return jsonify({"loi": "Không có dữ liệu với bộ lọc này"}), 404

    # Tổng số order của TOÀN BỘ dataset (mẫu số cố định, không filter)
    tong_order_toan_bo = df['SalesOrderID'].nunique()

    ket_qua = (
        du_lieu.groupby('ProductID')
        .agg(
            tong_doanh_thu=('doanh_thu', 'sum'),
            tong_so_luong=('OrderQty', 'sum'),
            don_gia_tb=('UnitPrice', 'mean'),
            so_don_hang=('SalesOrderID', 'nunique')
        )
        .reset_index()
        .rename(columns={'ProductID': 'ma_san_pham'})
        .sort_values('tong_doanh_thu', ascending=False)
    )

    ket_qua['tong_doanh_thu'] = ket_qua['tong_doanh_thu'].round(2)
    ket_qua['don_gia_tb']     = ket_qua['don_gia_tb'].round(2)
    # Tỷ lệ % order: số order theo SP / tổng order toàn bộ * 100
    ket_qua['ty_le_order_pct'] = (ket_qua['so_don_hang'] / tong_order_toan_bo * 100).round(2)

    return jsonify({
        "mo_ta": "Tổng doanh thu theo từng sản phẩm (doanh_thu = UnitPrice * OrderQty)",
        "tong_san_pham": len(ket_qua),
        "tong_order_toan_bo": tong_order_toan_bo,
        "du_lieu": ket_qua.to_dict(orient='records')
    })


# =============================================
# API: Download dữ liệu dạng CSV
# URL: /api/download-csv
# Query params (tùy chọn): ids (giống API trên)
# =============================================
@app.route('/api/download-csv')
def download_csv():
    ids_param = request.args.get('ids')

    du_lieu = df.copy()

    if ids_param:
        try:
            id_list = [int(x.strip()) for x in ids_param.split(',') if x.strip()]
            du_lieu = du_lieu[du_lieu['ProductID'].isin(id_list)]
        except ValueError:
            return jsonify({"loi": "ids không hợp lệ"}), 400

    ket_qua = (
        du_lieu.groupby('ProductID')
        .agg(
            tong_doanh_thu=('doanh_thu', 'sum'),
            tong_so_luong=('OrderQty', 'sum'),
            don_gia_tb=('UnitPrice', 'mean'),
            so_don_hang=('SalesOrderID', 'nunique')
        )
        .reset_index()
        .rename(columns={'ProductID': 'ma_san_pham'})
        .sort_values('tong_doanh_thu', ascending=False)
    )
    ket_qua['tong_doanh_thu'] = ket_qua['tong_doanh_thu'].round(2)
    ket_qua['don_gia_tb']     = ket_qua['don_gia_tb'].round(2)
    tong_order_toan_bo = df['SalesOrderID'].nunique()
    ket_qua['ty_le_order_pct'] = (ket_qua['so_don_hang'] / tong_order_toan_bo * 100).round(2)

    output = io.StringIO()
    ket_qua.to_csv(output, index=False)
    output.seek(0)

    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={"Content-Disposition": "attachment; filename=doanh_thu_san_pham.csv"}
    )


if __name__ == '__main__':
    app.run(debug=True, port=8080)


    # có thẻ dev giao diện 
