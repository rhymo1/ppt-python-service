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
import PyPDF2
import json
import re

app = Flask(__name__)

# Root route
@app.route('/')
def home():
    return jsonify({
        "status": "Python PPT Service is running",
        "version": "2.2",
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

# Helper: Extract text from PDF
def extract_text_from_pdf(pdf_base64):
    """Extract all text from a PDF file"""
    try:
        pdf_content = base64.b64decode(pdf_base64)
        pdf_file = BytesIO(pdf_content)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text()
        
        return text
    except Exception as e:
        return f"Error extracting PDF: {str(e)}"

# Helper: Extract data from sqproj
def extract_from_sqproj(sqproj_base64):
    """Extract data from .sqproj file (ZIP archive with XML/SQLite)"""
    try:
        sqproj_content = base64.b64decode(sqproj_base64)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.sqproj') as tmp:
            tmp.write(sqproj_content)
            tmp_path = tmp.name
        
        extracted = {}
        
        try:
            with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                for filename in zip_ref.namelist():
                    if filename.endswith('.xml'):
                        xml_content = zip_ref.read(filename)
                        try:
                            root = ET.fromstring(xml_content)
                            for elem in root.iter():
                                if elem.text and elem.text.strip():
                                    key = elem.tag.lower().replace(' ', '_')
                                    extracted[key] = elem.text.strip()
                        except:
                            pass
        except Exception as e:
            extracted["sqproj_error"] = str(e)
        
        os.unlink(tmp_path)
        
        return extracted
    
    except Exception as e:
        return {"error": f"Failed to process sqproj: {str(e)}"}

# Helper: Parse building data from PDF text
def parse_building_data(text):
    """Extract key building data from PDF text"""
    data = {}
    
    # Extract building address
    address_match = re.search(r'Gebäudeadresse[:\s]+([^\n]+)', text, re.IGNORECASE)
    if address_match:
        data['gebaeude_adresse'] = address_match.group(1).strip()
    
    # Extract year
    year_match = re.search(r'Baujahr[:\s]+(\d{4})', text, re.IGNORECASE)
    if year_match:
        data['baujahr'] = year_match.group(1)
    
    # Extract building type
    type_match = re.search(r'Gebäudetyp[:\s]+([^\n]+)', text, re.IGNORECASE)
    if type_match:
        data['gebaeude_typ'] = type_match.group(1).strip()
    
    # Extract living area
    area_match = re.search(r'Wohnfläche[:\s]+ca\.\s*([\d,\.]+)\s*m', text, re.IGNORECASE)
    if area_match:
        data['wohnflaeche'] = area_match.group(1).strip()
    
    # Extract energy costs
    costs_match = re.search(r'Energiekosten[:\s]+([\d,\.]+)\s*€', text, re.IGNORECASE)
    if costs_match:
        data['energiekosten_aktuell'] = costs_match.group(1).strip()
    
    # Extract CO2 emissions
    co2_match = re.search(r'CO[²2]-Emission[:\s]+([\d,]+)\s*kg', text, re.IGNORECASE)
    if co2_match:
        data['co2_emission_aktuell'] = co2_match.group(1).strip()
    
    return data

# Extract data from .sqproj and PDFs
@app.route('/extract', methods=['POST'])
def extract_data():
    try:
        data = request.json
        
        if not data:
            return jsonify({"error": "No JSON data provided"}), 400
        
        extracted_data = {}
        processing_log = []
        
        # 1. Process .sqproj file
        if 'sqproj_file' in data and data['sqproj_file']:
            processing_log.append("Processing .sqproj file...")
            sqproj_data = extract_from_sqproj(data['sqproj_file'])
            extracted_data.update(sqproj_data)
            processing_log.append(f"Extracted {len(sqproj_data)} fields from sqproj")
        else:
            processing_log.append("WARNING: No sqproj_file provided")
        
        # 2. Process Sanierungsfahrplan PDF
        if 'sanierungsfahrplan_pdf' in data and data['sanierungsfahrplan_pdf']:
            processing_log.append("Processing Sanierungsfahrplan PDF...")
            pdf_text = extract_text_from_pdf(data['sanierungsfahrplan_pdf'])
            parsed_data = parse_building_data(pdf_text)
            extracted_data.update(parsed_data)
            processing_log.append(f"Extracted {len(parsed_data)} fields from Sanierungsfahrplan")
            
            # Store full text for AI processing (if needed later)
            extracted_data['sanierungsfahrplan_text'] = pdf_text[:5000]  # First 5000 chars
        else:
            processing_log.append("WARNING: No sanierungsfahrplan_pdf provided")
        
        # 3. Process Umsetzungshilfe PDF
        if 'umsetzungshilfe_pdf' in data and data['umsetzungshilfe_pdf']:
            processing_log.append("Processing Umsetzungshilfe PDF...")
            pdf_text = extract_text_from_pdf(data['umsetzungshilfe_pdf'])
            parsed_data = parse_building_data(pdf_text)
            extracted_data.update(parsed_data)
            processing_log.append(f"Extracted {len(parsed_data)} fields from Umsetzungshilfe")
            
            # Store full text
            extracted_data['umsetzungshilfe_text'] = pdf_text[:5000]
        else:
            processing_log.append("WARNING: No umsetzungshilfe_pdf provided")
        
        # Add some default placeholders for testing
        extracted_data.update({
            "kunde_name": "Herr Willi Waschbär",
            "projekt_datum": datetime.now().strftime("%d.%m.%Y"),
            "berater_name": "Karl Sonvomdach",
            "berater_firma": "ProEco Rheinland GmbH & Co. KG"
        })
        
        return jsonify({
            "success": True,
            "extracted_data": extracted_data,
            "placeholder_count": len(extracted_data),
            "processing_log": processing_log,
            "files_processed": {
                "sqproj": bool(data.get('sqproj_file')),
                "sanierungsfahrplan_pdf": bool(data.get('sanierungsfahrplan_pdf')),
                "umsetzungshilfe_pdf": bool(data.get('umsetzungshilfe_pdf'))
            }
        })
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# Generate PPT from template
@app.route('/generate', methods=['POST'])
def generate_ppt():
    try:
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
        
        # Track replacements
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
                img_data = base64.b64decode(base64_image)
                img_stream = BytesIO(img_data)
                
                left = shape.left
                top = shape.top
                width = shape.width
                height = shape.height
                
                sp = shape.element
                sp.getparent().remove(sp)
                
                slide.shapes.add_picture(img_stream, left, top, width, height)
                
                return True
            except Exception as e:
                replacements_made["skipped"].append(f"{placeholder}: {str(e)}")
                return False
        
        # Process all slides
        for slide_idx, slide in enumerate(prs.slides):
            shapes_to_process = list(slide.shapes)
            
            for shape in shapes_to_process:
                try:
                    shape_text = ""
                    if shape.has_text_frame:
                        shape_text = shape.text_frame.text
                    elif hasattr(shape, 'text'):
                        shape_text = shape.text
                    
                    shape_removed = False
                    
                    for placeholder_key, placeholder_value in approved_data.items():
                        if shape_removed:
                            break
                        
                        placeholder_patterns = [
                            f"{{{{{placeholder_key}}}}}",
                            f"{{{placeholder_key}}}",
                            f"<<{placeholder_key}>>",
                        ]
                        
                        for placeholder_pattern in placeholder_patterns:
                            if placeholder_pattern in shape_text:
                                if placeholder_key.startswith("img_"):
                                    if isinstance(placeholder_value, str) and len(placeholder_value) > 100:
                                        if replace_image_placeholder(slide, shape, placeholder_pattern, placeholder_value):
                                            replacements_made["images"] += 1
                                            shape_removed = True
                                            break
                                    else:
                                        replacements_made["skipped"].append(f"{placeholder_key}: Empty or invalid image data")
                                else:
                                    if replace_text_in_shape(shape, placeholder_pattern, placeholder_value):
                                        replacements_made["text"] += 1
                                
                                break
                
                except Exception as e:
                    replacements_made["skipped"].append(f"Shape error on slide {slide_idx}: {str(e)}")
                    continue
        
        # Save presentation
        output_path = tempfile.mktemp(suffix='.pptx')
        prs.save(output_path)
        
        with open(output_path, 'rb') as f:
            file_content = f.read()
        
        os.unlink(template_path)
        os.unlink(output_path)
        
        file_base64 = base64.b64encode(file_content).decode('utf-8')
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
            "placeholders_received": len(approved_data)
        })
    
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 500

# Generate charts
@app.route('/generate-charts', methods=['POST'])
def generate_charts():
    try:
        data = request.json
        
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

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
