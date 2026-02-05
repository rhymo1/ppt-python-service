from flask import Flask, request, jsonify
import sqlite3
import base64
from pptx import Presentation
from io import BytesIO
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

app = Flask(__name__)

@app.route('/extract', methods=['POST'])
def extract_data():
    """Extract data from .sqproj file"""
    data = request.json
    sqproj_base64 = data['sqproj_file']
    
    # Decode and save .sqproj
    sqproj_bytes = base64.b64decode(sqproj_base64)
    with open('/tmp/temp.sqproj', 'wb') as f:
        f.write(sqproj_bytes)
    
    # Connect to SQLite
    conn = sqlite3.connect('/tmp/temp.sqproj')
    cursor = conn.cursor()
    
    # Extract building info
    cursor.execute("SELECT ShortDesc, YearOfConstruction, HeatableLivingArea FROM BmBuilding LIMIT 1")
    row = cursor.fetchone()
    building_info = {
        "name": row[0] if row else "Gebäude",
        "year": str(row[1])[:4] if row else "1958",
        "living_area": row[2] if row else 0
    }
    
    # Extract components
    cursor.execute("""
        SELECT ElementType, UValue, GrossArea 
        FROM BmElement 
        WHERE ElementType IN (1, 4, 5, 11) AND UValue > 0
    """)
    elements = cursor.fetchall()
    
    conn.close()
    
    # Process elements
    components = {}
    type_map = {1: "fenster", 4: "dach", 5: "aussenwand", 11: "keller"}
    
    for elem in elements:
        comp_type = type_map.get(elem[0])
        if comp_type:
            if comp_type not in components:
                components[comp_type] = {"areas": [], "u_values": []}
            components[comp_type]["areas"].append(elem[2])
            components[comp_type]["u_values"].append(elem[1])
    
    # Calculate averages
    result_components = {}
    for comp_type, data in components.items():
        total_area = sum(data["areas"])
        weighted_u = sum(u * a for u, a in zip(data["u_values"], data["areas"])) / total_area
        result_components[comp_type] = {
            "u_value_ist": round(weighted_u, 2),
            "total_area": round(total_area, 2)
        }
    
    return jsonify({
        "building_info": building_info,
        "components": result_components
    })

@app.route('/generate', methods=['POST'])
def generate_ppt():
    """Generate PPT from template and data"""
    data = request.json
    template_base64 = data['template_file']
    placeholders = data['placeholders']
    
    # Decode template
    template_bytes = base64.b64decode(template_base64)
    prs = Presentation(BytesIO(template_bytes))
    
    # Replace placeholders
    for slide in prs.slides:
        for shape in slide.shapes:
            if shape.has_text_frame:
                text = shape.text_frame.text
                for key, value in placeholders.items():
                    placeholder = f"{{{{{key}}}}}"
                    if placeholder in text:
                        shape.text = text.replace(placeholder, str(value))
    
    # Save to bytes
    output = BytesIO()
    prs.save(output)
    output.seek(0)
    
    return jsonify({
        "success": True,
        "file_content": base64.b64encode(output.read()).decode('utf-8'),
        "filename": "Sanierungsfahrplan.pptx"
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
