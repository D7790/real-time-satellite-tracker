import os
import time
import io
import csv
import requests
import pymysql
from pymysql.cursors import DictCursor
from pymysql.err import IntegrityError
from flask import Flask, jsonify, request, Response

# ----------------- MySQL config -----------------
DB_CFG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'sat_user',         # change if you used a different user
    'password': 'sat_pass',     # change if you used a different password
    'database': 'satellite_tracker',
    'cursorclass': DictCursor,
    'autocommit': True,
}

def open_db():
    return pymysql.connect(**DB_CFG)

# ----------------- Flask app -----------------
app = Flask(__name__, static_folder='static', static_url_path='/static')

@app.route('/')
def index():
    return app.send_static_file('index.html')

@app.route('/admin')
def admin_page():
    return app.send_static_file('admin.html')

# ----------------- DB init -----------------
def init_db():
    conn = open_db()
    c = conn.cursor()
    # satellites
    c.execute('''
        CREATE TABLE IF NOT EXISTS satellites (
          id INT AUTO_INCREMENT PRIMARY KEY,
          norad_id INT UNIQUE NOT NULL,
          name VARCHAR(100) NOT NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    # positions
    c.execute('''
        CREATE TABLE IF NOT EXISTS positions (
          id INT AUTO_INCREMENT PRIMARY KEY,
          satellite_id INT NOT NULL,
          `timestamp` INT NOT NULL,
          latitude DOUBLE NOT NULL,
          longitude DOUBLE NOT NULL,
          altitude_km DOUBLE NULL,
          velocity_kmh DOUBLE NULL,
          created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
          CONSTRAINT fk_positions_sat FOREIGN KEY (satellite_id)
            REFERENCES satellites(id) ON DELETE CASCADE
        )
    ''')
    # index (ignore error if exists)
    try:
        c.execute('CREATE INDEX idx_positions_sat_ts ON positions (satellite_id, `timestamp`)')
    except Exception:
        pass
    conn.close()

def get_satellite_id(norad_id, name):
    conn = open_db()
    c = conn.cursor()
    c.execute('SELECT id FROM satellites WHERE norad_id=%s', (norad_id,))
    row = c.fetchone()
    if row:
        sid = row['id']
    else:
        try:
            c.execute('INSERT INTO satellites (norad_id, name) VALUES (%s, %s)', (norad_id, name))
            sid = c.lastrowid
        except IntegrityError:
            # race condition safe fetch
            c.execute('SELECT id FROM satellites WHERE norad_id=%s', (norad_id,))
            sid = c.fetchone()['id']
    conn.close()
    return sid

# ----------------- APIs: ISS + history + status + CSV -----------------
@app.get('/api/iss')
def api_iss():
    norad_id = 25544
    url = 'https://api.wheretheiss.at/v1/satellites/25544'
    try:
        r = requests.get(url, timeout=8)
        r.raise_for_status()
        data = r.json()
    except Exception:
        # Fallback to last known position from DB if API fails
        conn = open_db()
        c = conn.cursor()
        c.execute('''
            SELECT p.`timestamp`, p.latitude, p.longitude, p.altitude_km, p.velocity_kmh
            FROM positions p
            JOIN satellites s ON s.id = p.satellite_id
            WHERE s.norad_id=%s
            ORDER BY p.`timestamp` DESC LIMIT 1
        ''', (norad_id,))
        row = c.fetchone()
        conn.close()
        if row:
            return jsonify({
                'source': 'cache',
                'timestamp': row['timestamp'],
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'altitude_km': row.get('altitude_km'),
                'velocity_kmh': row.get('velocity_kmh'),
            }), 200
        return jsonify({'error': 'Failed to fetch ISS position'}), 502

    sid = get_satellite_id(norad_id, data.get('name', 'ISS'))

    ts = int(float(data['timestamp']))
    lat = float(data['latitude'])
    lon = float(data['longitude'])
    alt = float(data.get('altitude', 0))
    vel = float(data.get('velocity', 0))  # km/h

    conn = open_db()
    c = conn.cursor()
    c.execute('''
        INSERT INTO positions (satellite_id, `timestamp`, latitude, longitude, altitude_km, velocity_kmh)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (sid, ts, lat, lon, alt, vel))
    conn.close()

    return jsonify({
        'source': 'live',
        'timestamp': ts,
        'latitude': lat,
        'longitude': lon,
        'altitude_km': alt,
        'velocity_kmh': vel,
    })

@app.get('/api/history')
def api_history():
    norad_id = 25544
    limit = int(request.args.get('limit', 50))
    limit = max(1, min(limit, 1000))
    conn = open_db()
    c = conn.cursor()
    c.execute('''
        SELECT p.`timestamp`, p.latitude, p.longitude, p.altitude_km, p.velocity_kmh
        FROM positions p
        JOIN satellites s ON s.id = p.satellite_id
        WHERE s.norad_id=%s
        ORDER BY p.`timestamp` DESC
        LIMIT %s
    ''', (norad_id, limit))
    rows = c.fetchall()
    conn.close()
    rows.reverse()  # oldest -> newest
    return jsonify(rows)

@app.get('/api/status')
def api_status():
    norad_id = 25544
    conn = open_db()
    c = conn.cursor()
    c.execute('''
      SELECT COUNT(*) AS cnt, MIN(p.`timestamp`) AS min_ts, MAX(p.`timestamp`) AS max_ts
      FROM positions p
      JOIN satellites s ON s.id = p.satellite_id
      WHERE s.norad_id=%s
    ''', (norad_id,))
    row = c.fetchone() or {'cnt': 0, 'min_ts': None, 'max_ts': None}
    conn.close()
    return jsonify({
        'points': int(row['cnt'] or 0),
        'first_timestamp': row['min_ts'],
        'last_timestamp': row['max_ts']
    })

@app.get('/api/history.csv')
def api_history_csv():
    norad_id = 25544
    limit = int(request.args.get('limit', 500))
    limit = max(1, min(limit, 5000))

    conn = open_db()
    c = conn.cursor()
    c.execute('''
        SELECT p.`timestamp`, p.latitude, p.longitude, p.altitude_km, p.velocity_kmh
        FROM positions p
        JOIN satellites s ON s.id = p.satellite_id
        WHERE s.norad_id=%s
        ORDER BY p.`timestamp` DESC
        LIMIT %s
    ''', (norad_id, limit))
    rows = c.fetchall()
    conn.close()

    rows.reverse()
    out = io.StringIO()
    writer = csv.writer(out)
    writer.writerow(['timestamp', 'latitude', 'longitude', 'altitude_km', 'velocity_kmh'])
    for r in rows:
        writer.writerow([r['timestamp'], r['latitude'], r['longitude'], r.get('altitude_km'), r.get('velocity_kmh')])
    csv_data = out.getvalue()
    return Response(
        csv_data,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=iss_history.csv'}
    )

# ----------------- CRUD: Satellites -----------------
@app.get('/api/satellites')
def list_satellites():
    conn = open_db()
    c = conn.cursor()
    c.execute('''
        SELECT s.id, s.norad_id, s.name, s.created_at,
               (SELECT COUNT(*) FROM positions p WHERE p.satellite_id = s.id) AS position_count
        FROM satellites s
        ORDER BY s.id ASC
    ''')
    rows = c.fetchall()
    conn.close()
    return jsonify(rows)

@app.post('/api/satellites')
def create_satellite():
    data = request.get_json(silent=True) or request.form
    name = (data.get('name') or '').strip()
    norad = data.get('norad_id')
    if not name or norad is None:
        return jsonify({'error': 'name and norad_id are required'}), 400
    try:
        norad = int(norad)
    except ValueError:
        return jsonify({'error': 'norad_id must be integer'}), 400
    conn = open_db()
    c = conn.cursor()
    try:
        c.execute('INSERT INTO satellites (norad_id, name) VALUES (%s, %s)', (norad, name))
        sid = c.lastrowid
    except IntegrityError:
        conn.close()
        return jsonify({'error': 'norad_id already exists'}), 409
    conn.close()
    return jsonify({'id': sid, 'norad_id': norad, 'name': name}), 201

@app.put('/api/satellites/<int:sid>')
def update_satellite(sid):
    data = request.get_json(silent=True) or request.form
    fields, values = [], []
    if 'name' in data:
        fields.append('name=%s'); values.append((data['name'] or '').strip())
    if 'norad_id' in data:
        try:
            values.append(int(data['norad_id'])); fields.append('norad_id=%s')
        except ValueError:
            return jsonify({'error': 'norad_id must be integer'}), 400
    if not fields:
        return jsonify({'error': 'no fields to update'}), 400
    values.append(sid)
    conn = open_db()
    c = conn.cursor()
    try:
        c.execute(f'UPDATE satellites SET {", ".join(fields)} WHERE id=%s', values)
    except IntegrityError:
        conn.close()
        return jsonify({'error': 'norad_id already exists'}), 409
    if c.rowcount == 0:
        conn.close()
        return jsonify({'error': 'satellite not found'}), 404
    conn.close()
    return jsonify({'updated': True})

@app.delete('/api/satellites/<int:sid>')
def delete_satellite(sid):
    conn = open_db()
    c = conn.cursor()
    c.execute('DELETE FROM satellites WHERE id=%s', (sid,))
    deleted = c.rowcount
    conn.close()
    if deleted == 0:
        return jsonify({'error': 'satellite not found'}), 404
    return jsonify({'deleted': True})

# ----------------- CRUD: Positions -----------------
@app.get('/api/positions')
def list_positions():
    sat_id = request.args.get('satellite_id')
    norad = request.args.get('norad_id')
    limit = int(request.args.get('limit', 50))
    limit = max(1, min(limit, 1000))
    conn = open_db()
    c = conn.cursor()

    if norad and not sat_id:
        try:
            norad = int(norad)
        except ValueError:
            conn.close()
            return jsonify({'error': 'norad_id must be integer'}), 400
        c.execute('SELECT id FROM satellites WHERE norad_id=%s', (norad,))
        r = c.fetchone()
        if not r:
            conn.close()
            return jsonify([])
        sat_id = r['id']

    if not sat_id:
        conn.close()
        return jsonify({'error': 'satellite_id or norad_id required'}), 400
    try:
        sat_id = int(sat_id)
    except ValueError:
        conn.close()
        return jsonify({'error': 'satellite_id must be integer'}), 400

    c.execute('''
        SELECT id, satellite_id, `timestamp`, latitude, longitude, altitude_km, velocity_kmh, created_at
        FROM positions
        WHERE satellite_id=%s
        ORDER BY `timestamp` DESC
        LIMIT %s
    ''', (sat_id, limit))
    rows = c.fetchall()
    conn.close()
    return jsonify(rows)

@app.post('/api/positions')
def create_position():
    data = request.get_json(silent=True) or request.form
    if not data:
        return jsonify({'error': 'no data provided'}), 400
    sat_id = data.get('satellite_id')
    try:
        sat_id = int(sat_id)
    except (TypeError, ValueError):
        return jsonify({'error': 'satellite_id is required (int)'}), 400

    ts = data.get('timestamp')
    if ts in (None, '', '0', 0):
        ts = int(time.time())
    else:
        try:
            ts = int(ts)
        except ValueError:
            return jsonify({'error': 'timestamp must be unix seconds'}), 400

    try:
        lat = float(data.get('latitude'))
        lon = float(data.get('longitude'))
    except (TypeError, ValueError):
        return jsonify({'error': 'latitude and longitude are required numbers'}), 400

    alt = data.get('altitude_km')
    vel = data.get('velocity_kmh')
    alt_val = None if alt in (None, '') else float(alt)
    vel_val = None if vel in (None, '') else float(vel)

    conn = open_db()
    c = conn.cursor()
    c.execute('SELECT 1 FROM satellites WHERE id=%s', (sat_id,))
    if not c.fetchone():
        conn.close()
        return jsonify({'error': 'satellite not found'}), 404

    c.execute('''
        INSERT INTO positions (satellite_id, `timestamp`, latitude, longitude, altitude_km, velocity_kmh)
        VALUES (%s, %s, %s, %s, %s, %s)
    ''', (sat_id, ts, lat, lon, alt_val, vel_val))
    pid = c.lastrowid
    conn.close()
    return jsonify({
        'id': pid, 'satellite_id': sat_id, 'timestamp': ts,
        'latitude': lat, 'longitude': lon,
        'altitude_km': alt_val, 'velocity_kmh': vel_val
    }), 201

@app.put('/api/positions/<int:pid>')
def update_position_row(pid):
    data = request.get_json(silent=True) or request.form
    fields, values = [], []
    if 'timestamp' in data:
        try:
            values.append(int(data['timestamp'])); fields.append('`timestamp`=%s')
        except ValueError:
            return jsonify({'error': 'timestamp must be int'}), 400
    if 'latitude' in data:
        try:
            values.append(float(data['latitude'])); fields.append('latitude=%s')
        except ValueError:
            return jsonify({'error': 'latitude must be number'}), 400
    if 'longitude' in data:
        try:
            values.append(float(data['longitude'])); fields.append('longitude=%s')
        except ValueError:
            return jsonify({'error': 'longitude must be number'}), 400
    if 'altitude_km' in data:
        v = None if data['altitude_km'] in (None, '') else float(data['altitude_km'])
        values.append(v); fields.append('altitude_km=%s')
    if 'velocity_kmh' in data:
        v = None if data['velocity_kmh'] in (None, '') else float(data['velocity_kmh'])
        values.append(v); fields.append('velocity_kmh=%s')

    if not fields:
        return jsonify({'error': 'no fields to update'}), 400

    values.append(pid)
    conn = open_db()
    c = conn.cursor()
    c.execute(f'UPDATE positions SET {", ".join(fields)} WHERE id=%s', values)
    if c.rowcount == 0:
        conn.close()
        return jsonify({'error': 'position not found'}), 404
    conn.close()
    return jsonify({'updated': True})

@app.delete('/api/positions/<int:pid>')
def delete_position_row(pid):
    conn = open_db()
    c = conn.cursor()
    c.execute('DELETE FROM positions WHERE id=%s', (pid,))
    deleted = c.rowcount
    conn.close()
    if deleted == 0:
        return jsonify({'error': 'position not found'}), 404
    return jsonify({'deleted': True})

# ----------------- Startup -----------------
def setup_app():
    init_db()
    get_satellite_id(25544, 'ISS')  # seed ISS

if __name__ == '__main__':
    setup_app()
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, port=port)