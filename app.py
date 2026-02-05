from flask import Flask, request, jsonify
from pptx import Presentation
import zipfile
import xml.etree.ElementTree as ET
import os
import tempfile
from datetime import datetime

app = Flask(__name__)

# Root route
@app.route('/')
def home():
    return jsonify({
        "status": "Python PPT Service is running",
        "version": "1.0",
        "endpoints": [
            "/health",
            "/extract",
            "/generate"
        ]
    })

# Health check route
@app.route('/health')
def health():
    return jsonify({
        "status": "healthy",
        "timestamp": datetime.now().isoformat()
    })

# Extract data from .sqproj file
@app.route('/extract', methods=['POST'])
def extract_data():
    try:
        # Get file from request
        if 'file' not in request.files:
            return jsonify({"error": "No file provided"}), 400
        
        file = request.files['file']
        
        # Save temporarily
        with tempfile.NamedTemporaryFile(delete=False, suffix='.sqproj') as tmp:
            file.save(tmp.name)
            tmp_path = tmp.name
        
        # Extract XML from sqproj
        with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
            # Find the main data XML file
            xml_content = None
            for filename in zip_ref.namelist():
                if filename.endswith('.xml'):
                    xml_content = zip_ref.read(filename)
                    break
        
        # Clean up temp file
        os.unlink(tmp_path)
        
        if not xml_content:
            return jsonify({"error": "No XML found in sqproj file"}), 400
        
        # Parse XML
        root = ET.fromstring(xml_content)
        
        # Extract data (customize based on your XML structure)
        extracted_data = {}
        for elem in root.iter():
            if elem.text and elem.text.strip():
                extracted_data[elem.tag] = elem.text.strip()
        
        return jsonify({
            "status": "success",
            "data": extracted_data
        })
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Generate PPT from template
@app.route('/generate', methods=['POST'])
def generate_ppt():
    try:
        # Get data and template from request
        data = request.json
        
        if 'template_path' not in data or 'replacements' not in data:
            return jsonify({"error": "Missing template_path or replacements"}), 400
        
        template_path = data['template_path']
        replacements = data['replacements']
        
        # Load template
        if not os.path.exists(template_path):
            return jsonify({"error": f"Template not found: {template_path}"}), 404
        
        prs = Presentation(template_path)
        
        # Replace placeholders in all slides
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, "text"):
                    for placeholder, value in replacements.items():
                        if placeholder in shape.text:
                            shape.text = shape.text.replace(placeholder, str(value))
        
        # Save to temporary file
        output_path = tempfile.mktemp(suffix='.pptx')
        prs.save(output_path)
        
        # Read file content
        with open(output_path, 'rb') as f:
            file_content = f.read()
        
        # Clean up
        os.unlink(output_path)
        
        # Return file as base64 (or you could return as file download)
        import base64
        encoded_content = base64.b64encode(file_content).decode('utf-8')
        
        return jsonify({
            "status": "success",
            "file_content": encoded_content,
            "filename": f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pptx"
        })
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
