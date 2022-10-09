from flask import Flask, request, Response

from utils import get_data_set


app = Flask(__name__)


@app.route('/api', methods=['GET'])
def get_data_set():
    res_csv = get_data_set(**request.args)
    return Response(
        res_csv,
        mimetype='text/csv',
        headers={
            "Content-disposition":
            "attachment; filename=result.csv"
        })


if __name__ == "__main__":
    app.run(debug=True)
