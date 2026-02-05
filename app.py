from flask import Flask, request, jsonify
import zipfile
import xml.etree.ElementTree as ET
import os
import tempfile
import base64
from datetime import datetime
from io import BytesIO
from pptx import Presentation
from PIL import Image

app = Flask(__name__)

# Root route
@app.route('/')
def home():
    return jsonify({
        "status": "Python PPT Service is running",
        "version": "2.0",
        "endpoints": [
            "/health",
            "/extract",
            "/generate",
            "/generate-charts"
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
        # Get data from request
        data = request.json
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        if 'template_file' not in data:
            return jsonify({"error": "Missing template_file (base64)"}), 400
        
        if 'approved_data' not in data:
            return jsonify({"error": "Missing approved_data"}), 400
        
        template_base64 = data['template_file']
        approved_data = data['approved_data']
        template_filename = data.get('template_filename', 'template.pptx')
        
        # Decode base64 template
        try:
            template_content = base64.b64decode(template_base64)
        except Exception as e:
            return jsonify({"error": f"Invalid base64 template data: {str(e)}"}), 400
        
        # Save template to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pptx') as tmp_template:
            tmp_template.write(template_content)
            template_path = tmp_template.name
        
        # Load presentation
        try:
            prs = Presentation(template_path)
        except Exception as e:
            os.unlink(template_path)
            return jsonify({"error": f"Failed to load template: {str(e)}"}), 400
        
        # Track replacements for debugging
        replacements_made = {
            "text": 0,
            "images": 0,
            "skipped": []
        }
        
        # Function to replace text in shape
        def replace_text_in_shape(shape, placeholder, value):
            replaced = False
            
            if shape.has_text_frame:
                text_frame = shape.text_frame
                for paragraph in text_frame.paragraphs:
                    for run in paragraph.runs:
                        if placeholder in run.text:
                            run.text = run.text.replace(placeholder, str(value))
                            replaced = True
            
            # Also check in shape.text directly
            if hasattr(shape, 'text') and placeholder in shape.text:
                try:
                    shape.text = shape.text.replace(placeholder, str(value))
                    replaced = True
                except:
                    pass
            
            return replaced
        
        # Function to replace image placeholder
        def replace_image_placeholder(slide, shape, placeholder, base64_image):
            try:
                # Decode base64 image
                img_data = base64.b64decode(base64_image)
                img_stream = BytesIO(img_data)
                
                # Get position and size from the placeholder shape
                left = shape.left
                top = shape.top
                width = shape.width
                height = shape.height
                
                # Remove the placeholder shape
                sp = shape.element
                sp.getparent().remove(sp)
                
                # Add image to slide at the same position
                slide.shapes.add_picture(img_stream, left, top, width, height)
                
                return True
            except Exception as e:
                replacements_made["skipped"].append(f"{placeholder}: {str(e)}")
                return False
        
        # Iterate through all slides
        for slide_idx, slide in enumerate(prs.slides):
            shapes_to_process = list(slide.shapes)  # Create a copy
            
            for shape in shapes_to_process:
                try:
                    # Check if shape has text
                    shape_text = ""
                    if shape.has_text_frame:
                        shape_text = shape.text_frame.text
                    elif hasattr(shape, 'text'):
                        shape_text = shape.text
                    
                    # Flag to track if shape was removed
                    shape_removed = False
                    
                    # Process each placeholder
                    for placeholder_key, placeholder_value in approved_data.items():
                        # Skip if shape was already removed
                        if shape_removed:
                            break
                        
                        # Support multiple placeholder formats
                        placeholder_patterns = [
                            f"{{{{{placeholder_key}}}}}",  # {{placeholder}}
                            f"{{{placeholder_key}}}",       # {placeholder}
                            f"<<{placeholder_key}>>",        # <<placeholder>>
                        ]
                        
                        for placeholder_pattern in placeholder_patterns:
                            if placeholder_pattern in shape_text:
                                # Check if it's an image placeholder
                                if placeholder_key.startswith("img_"):
                                    # Image replacement
                                    if isinstance(placeholder_value, str) and len(placeholder_value) > 100:
                                        # It's base64 image data
                                        if replace_image_placeholder(slide, shape, placeholder_pattern, placeholder_value):
                                            replacements_made["images"] += 1
                                            shape_removed = True
                                            break
                                    else:
                                        replacements_made["skipped"].append(f"{placeholder_key}: Empty or invalid image data")
                                else:
                                    # Text replacement
                                    if replace_text_in_shape(shape, placeholder_pattern, placeholder_value):
                                        replacements_made["text"] += 1
                                
                                # Break after finding first matching pattern
                                break
                
                except Exception as e:
                    replacements_made["skipped"].append(f"Shape error on slide {slide_idx}: {str(e)}")
                    continue
        
        # Save filled presentation
        output_path = tempfile.mktemp(suffix='.pptx')
        prs.save(output_path)
        
        # Read file content
        with open(output_path, 'rb') as f:
            file_content = f.read()
        
        # Clean up temp files
        os.unlink(template_path)
        os.unlink(output_path)
        
        # Encode to base64
        file_base64 = base64.b64encode(file_content).decode('utf-8')
        
        # Get file size
        file_size = len(file_content)
        file_size_mb = round(file_size / (1024 * 1024), 2)
        
        return jsonify({
            "success": True,
            "filename": f"Sanierungsfahrplan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pptx",
            "file_content": file_base64,
            "mimetype": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "file_size_bytes": file_size,
            "file_size_mb": file_size_mb,
            "replacements": replacements_made,
            "slides_processed": len(prs.slides),
            "placeholders_received": len(approved_data),
            "download_url": f"data:application/vnd.openxmlformats-officedocument.presentationml.presentation;base64,{file_base64}"
        })
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Generate charts (placeholder - will implement with matplotlib)
@app.route('/generate-charts', methods=['POST'])
def generate_charts():
    try:
        data = request.json
        
        # This will be implemented later with matplotlib
        return jsonify({
            "status": "success",
            "message": "Chart generation endpoint - to be implemented",
            "images": {}
        })
    
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# Run the Flask app
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
