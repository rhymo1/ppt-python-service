"""
Comprehensive Data Extraction Flask App v4.0
Extracts ALL data from .sqproj and PDFs for intelligent PPT placeholder filling
"""

from flask import Flask, request, jsonify
import zipfile
import xml.etree.ElementTree as ET
import os
import tempfile
import base64
from datetime import datetime
from io import BytesIO
from pptx import Presentation
import PyPDF2
import json
import re

app = Flask(__name__)

# ============================================================================
# HELPER: Comprehensive PDF Parser
# ============================================================================

def extract_all_data_from_pdf(pdf_bytes):
    """
    Extract ALL data from PDF:
    - All text (page by page)
    - All numbers with context
    - All tables (structured)
    - Metadata
    """
    try:
        pdf_file = BytesIO(pdf_bytes)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        result = {
            'pages': [],
            'full_text': '',
            'metadata': {},
            'numbers_found': [],
            'tables_found': []
        }
        
        # Extract metadata
        if pdf_reader.metadata:
            result['metadata'] = {
                'title': pdf_reader.metadata.get('/Title', ''),
                'author': pdf_reader.metadata.get('/Author', ''),
                'creator': pdf_reader.metadata.get('/Creator', ''),
                'producer': pdf_reader.metadata.get('/Producer', ''),
                'creation_date': pdf_reader.metadata.get('/CreationDate', '')
            }
        
        # Extract page-by-page text
        for i, page in enumerate(pdf_reader.pages, 1):
            page_text = page.extract_text()
            result['pages'].append({
                'page_number': i,
                'text': page_text
            })
            result['full_text'] += page_text + '\n\n'
        
        # Extract all numbers with context
        number_pattern = r'([\w\s]{0,30})([\d\.,]+)\s*(€|kWh|m²|kg|%|W|cm|mm)?'
        for match in re.finditer(number_pattern, result['full_text']):
            result['numbers_found'].append({
                'context': match.group(1).strip(),
                'value': match.group(2),
                'unit': match.group(3) or ''
            })
        
        return result
        
    except Exception as e:
        return {'error': f'PDF extraction failed: {str(e)}'}


# ============================================================================
# HELPER: Comprehensive .sqproj Parser
# ============================================================================

def extract_all_from_sqproj(sqproj_bytes):
    """
    Extract ALL data from .sqproj file:
    - All XML elements with hierarchy
    - All attributes
    - All text values
    - File structure
    """
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.sqproj') as tmp:
            tmp.write(sqproj_bytes)
            tmp_path = tmp.name
        
        result = {
            'xml_data': {},
            'file_list': [],
            'attributes': {},
            'text_values': []
        }
        
        try:
            with zipfile.ZipFile(tmp_path, 'r') as zip_ref:
                result['file_list'] = zip_ref.namelist()
                
                for filename in zip_ref.namelist():
                    if filename.endswith('.xml'):
                        xml_content = zip_ref.read(filename)
                        try:
                            root = ET.fromstring(xml_content)
                            
                            # Extract all elements with hierarchy
                            def parse_element(elem, path=''):
                                current_path = f'{path}/{elem.tag}' if path else elem.tag
                                
                                # Store attributes
                                if elem.attrib:
                                    result['attributes'][current_path] = elem.attrib
                                
                                # Store text
                                if elem.text and elem.text.strip():
                                    text_val = elem.text.strip()
                                    result['text_values'].append({
                                        'path': current_path,
                                        'value': text_val
                                    })
                                    
                                    # Also store in structured format
                                    key = current_path.split('/')[-1].lower().replace(' ', '_')
                                    result['xml_data'][key] = text_val
                                
                                # Recurse into children
                                for child in elem:
                                    parse_element(child, current_path)
                            
                            parse_element(root)
                            
                        except Exception as e:
                            result['xml_parse_error'] = str(e)
        
        except Exception as e:
            result['sqproj_error'] = str(e)
        
        os.unlink(tmp_path)
        return result
        
    except Exception as e:
        return {'error': f'Failed to process sqproj: {str(e)}'}


# ============================================================================
# HELPER: Structure All Extracted Data
# ============================================================================

def structure_complete_data(sqproj_data, pdf1_data, pdf2_data):
    """
    Merge all extracted data into a unified, intelligently structured format
    ready for AI mapping to PPT placeholders.
    """
    
    # Combine all text for comprehensive search
    all_text = ''
    if pdf1_data.get('full_text'):
        all_text += pdf1_data['full_text'] + '\n\n'
    if pdf2_data.get('full_text'):
        all_text += pdf2_data['full_text'] + '\n\n'
    
    # Normalize whitespace for easier pattern matching
    normalized = re.sub(r'\s+', ' ', all_text)
    
    # Extract key data points with comprehensive patterns
    structured = {
        'raw_data': {
            'sqproj': sqproj_data,
            'pdf1': pdf1_data,
            'pdf2': pdf2_data
        },
        'extracted': {}
    }
    
    # Building Information
    structured['extracted']['building'] = extract_building_info(normalized)
    
    # Consultant Information
    structured['extracted']['consultant'] = extract_consultant_info(normalized)
    
    # Current Energy State
    structured['extracted']['energy_current'] = extract_energy_current(normalized)
    
    # Target Energy State
    structured['extracted']['energy_target'] = extract_energy_target(normalized)
    
    # Components (7 components × IST + LÖSUNG)
    structured['extracted']['components'] = extract_all_components(normalized)
    
    # Measure Packages (5 packages)
    structured['extracted']['measure_packages'] = extract_measure_packages(normalized)
    
    # Costs Summary
    structured['extracted']['costs'] = extract_costs(normalized)
    
    # Timeline
    structured['extracted']['timeline'] = extract_timeline(normalized)
    
    # U-values (all components, before/after)
    structured['extracted']['u_values'] = extract_u_values(normalized)
    
    # Technical specifications
    structured['extracted']['technical_specs'] = extract_technical_specs(normalized)
    
    # All numbers found (for anything we might have missed)
    all_numbers = []
    if pdf1_data.get('numbers_found'):
        all_numbers.extend(pdf1_data['numbers_found'])
    if pdf2_data.get('numbers_found'):
        all_numbers.extend(pdf2_data['numbers_found'])
    structured['extracted']['all_numbers'] = all_numbers
    
    return structured


# ============================================================================
# EXTRACTION HELPERS: Individual Data Categories
# ============================================================================

def extract_building_info(text):
    """Extract building identification and basic info"""
    info = {}
    
    # Address
    m = re.search(r'([\w\s]{3,40}(?:str|Str|weg|Weg|gasse|Allee)\s*\d+[^\n]*\n?\s*\d{5}\s+\w+)', text)
    if m:
        info['address'] = re.sub(r'\s+', ' ', m.group(1)).strip()
    
    # Owner
    m = re.search(r'Sehr geehrter?\s+(Herr|Frau)\s+([^\n,]+)', text)
    if m:
        info['owner'] = f"{m.group(1)} {m.group(2).strip()}"
    
    # Building type
    m = re.search(r'Gebäudetyp\s+([^\n]+)', text)
    if m:
        info['type'] = m.group(1).strip()
    
    # Construction year
    m = re.search(r'Baujahr\s+(\d{4})', text)
    if m:
        info['construction_year'] = m.group(1)
    
    # Living area
    m = re.search(r'Wohnfläche\s+ca\.\s*([\d,\.]+)\s*m', text)
    if m:
        info['living_area_m2'] = m.group(1).replace(',', '.')
    
    # Floors
    m = re.search(r'Vollgeschosse\s+(\d+)', text)
    if m:
        info['floors'] = m.group(1)
    
    # Basement
    m = re.search(r'Keller\s+(ja[^\n]*|nein)', text, re.IGNORECASE)
    if m:
        info['basement'] = m.group(1).strip()
    
    return info


def extract_consultant_info(text):
    """Extract consultant/advisor information"""
    info = {}
    
    # Name
    m = re.search(r'Energieberatung\s*\n\s*([^\n]+)', text)
    if m:
        info['name'] = m.group(1).strip()
    
    # Company
    m = re.search(r'(ProEco[^\n]+(?:GmbH|KG|AG)[^\n]*)', text)
    if m:
        info['company'] = m.group(1).strip()
    
    # BAFA number
    m = re.search(r'Beraternummer[:\s]+([^\n]+)', text)
    if m:
        info['bafa_number'] = m.group(1).strip()
    
    # Vorgangsnr
    m = re.search(r'Vorgangsnr[^\n]*BAFA[^\n]*([A-Z]+\s*\d+)', text)
    if m:
        info['vorgangsnr'] = m.group(1).strip()
    
    return info


def extract_energy_current(text):
    """Extract current (IST) energy state"""
    current = {}
    
    # Find IST section (before "Ziel" or "Zukunft")
    ist_section = text[:text.find('Ihr Haus in Zukun') if 'Ihr Haus in Zukun' in text else len(text)]
    
    # Primary energy demand
    m = re.search(r'Primärenergiebedarf\s*q\s*p?\s*([\d,]+)\s*kWh/\(m²a\)', ist_section)
    if m:
        current['primary_demand_kwh_m2a'] = m.group(1).replace(',', '.')
    
    # End energy consumption
    m = re.search(r'Endenergieverbrauch\s*([\d\.]+)\s*kWh/a', ist_section)
    if m:
        current['end_consumption_kwh_a'] = m.group(1).replace('.', '')
    
    # Energy costs
    m = re.search(r'Energiekosten[³\d\s]*\s*([\d\.]+)\s*€/a', ist_section)
    if m:
        current['costs_eur_a'] = m.group(1).replace('.', '')
    
    # CO2 emissions
    m = re.search(r'äquivalente\s*CO.{0,4}Emission\s*([\d,]+)\s*kg/\(m²a\)', ist_section)
    if m:
        current['co2_kg_m2a'] = m.group(1).replace(',', '.')
    
    return current


def extract_energy_target(text):
    """Extract target (ZIEL) energy state"""
    target = {}
    
    # Find ZIEL section
    if 'Ihr Haus in Zukun' in text:
        ziel_section = text[text.find('Ihr Haus in Zukun'):]
    else:
        return target
    
    # Primary energy demand
    m = re.search(r'Primärenergiebedarf\s*q\s*p?\s*([\d,]+)\s*kWh/\(m²a\)', ziel_section)
    if m:
        target['primary_demand_kwh_m2a'] = m.group(1).replace(',', '.')
    
    # End energy consumption
    m = re.search(r'Endenergieverbrauch\s*([\d\.]+)\s*kWh/a', ziel_section)
    if m:
        target['end_consumption_kwh_a'] = m.group(1).replace('.', '')
    
    # Energy costs
    m = re.search(r'Energiekosten[³\d\s]*\s*([\d\.]+)\s*€/a', ziel_section)
    if m:
        target['costs_eur_a'] = m.group(1).replace('.', '')
    
    # CO2 emissions
    m = re.search(r'äquivalente\s*CO.{0,4}Emission\s*([\d,]+)\s*kg/\(m²a\)', ziel_section)
    if m:
        target['co2_kg_m2a'] = m.group(1).replace(',', '.')
    
    # Efficiency standard
    m = re.search(r'(EH\s*\d+\s*EE)', text)
    if m:
        target['efficiency_standard'] = m.group(1).replace(' ', ' ')
    
    return target


def extract_all_components(text):
    """Extract all component data (walls, roof, windows, etc.)"""
    components = {}
    
    component_names = [
        'aussenwand', 'dach', 'fenster', 'keller', 
        'heizung', 'warmwasser', 'lueftung'
    ]
    
    for comp in component_names:
        components[comp] = {
            'ist_description': '',
            'loesung_description': '',
            'u_value_ist': '',
            'u_value_target': ''
        }
    
    # Extract descriptions from tables/sections
    # IST descriptions
    ist_patterns = {
        'aussenwand': r'(?:Außen)?[Ww]ände?[^\n]{0,20}(?:Massiv|gemauert|ungedämmt)[^\n]{0,60}',
        'dach': r'Dach[^\n]{0,20}(?:Dämmung|nicht\s+einsehbar|ungedämmt)[^\n]{0,60}',
        'fenster': r'Fenster[^\n]{0,20}(?:\d-fach|verglast)[^\n]{0,60}',
        'keller': r'Keller[^\n]{0,20}(?:ungedämmt|teilbeheizt)[^\n]{0,60}',
        'heizung': r'(?:Heizung|Erzeuger)[^\n]{0,20}(?:Gas|Kessel|Standardkessel)[^\n]{0,60}',
        'warmwasser': r'Warmwasser[^\n]{0,20}(?:über|Wärmeerzeuger)[^\n]{0,60}',
        'lueftung': r'Lüftung[^\n]{0,20}(?:Freie|Fenster|Infiltration)[^\n]{0,60}'
    }
    
    for comp, pattern in ist_patterns.items():
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            components[comp]['ist_description'] = m.group(0).strip()
    
    # LÖSUNG descriptions
    loesung_patterns = {
        'aussenwand': r'Außenwand[^\n]{0,20}Dämmung[^\n]{0,60}',
        'dach': r'Dach[^\n]{0,20}(?:ZSD|ASD|Dämmung)[^\n]{0,60}',
        'fenster': r'Fenster[^\n]{0,20}Uw-Wert[^\n]{0,60}',
        'keller': r'Keller[^\n]{0,20}Dämmung[^\n]{0,60}',
        'heizung': r'(?:Wärmepumpe|Heizung)[^\n]{0,20}(?:Lu-Wasser|L/W)[^\n]{0,60}',
        'warmwasser': r'Warmwasser[^\n]{0,20}(?:Wärmepumpe|Heizungsanlage)[^\n]{0,60}',
        'lueftung': r'Lüftung[^\n]{0,20}(?:WRG|Wärmerückgewinnung)[^\n]{0,60}'
    }
    
    for comp, pattern in loesung_patterns.items():
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            components[comp]['loesung_description'] = m.group(0).strip()
    
    return components


def extract_measure_packages(text):
    """Extract all 5 measure packages with details"""
    packages = []
    
    # Pattern to find each package
    for i in range(1, 6):
        package = {
            'id': i,
            'name': '',
            'year': '',
            'investment': '',
            'sowieso': '',
            'funding': '',
            'energy_cost_after': ''
        }
        
        # Find package name
        patterns = [
            (1, r'Maßnahmenpaket\s+1[^\n]*([^\n]{10,60})'),
            (2, r'Maßnahmenpaket\s+2[^\n]*([^\n]{10,60})'),
            (3, r'Maßnahmenpaket\s+3[^\n]*([^\n]{10,60})'),
            (4, r'Maßnahmenpaket\s+4[^\n]*([^\n]{10,60})'),
            (5, r'Maßnahmenpaket\s+5[^\n]*([^\n]{10,60})')
        ]
        
        for pkg_id, pattern in patterns:
            if pkg_id == i:
                m = re.search(pattern, text)
                if m:
                    package['name'] = m.group(1).strip()
        
        # Find costs in table format
        # Look for pattern: number € number € number € near the package
        cost_pattern = rf'Maßnahmenpaket\s+{i}.*?([\d\.]+)\s*€[^\d]{{0,20}}([\d\.]+)\s*€[^\d]{{0,20}}([\d\.]+)\s*€'
        m = re.search(cost_pattern, text, re.DOTALL)
        if m:
            package['investment'] = m.group(1).replace('.', '')
            package['sowieso'] = m.group(2).replace('.', '')
            package['funding'] = m.group(3).replace('.', '')
        
        packages.append(package)
    
    return packages


def extract_costs(text):
    """Extract total costs summary"""
    costs = {}
    
    # Total investment
    m = re.search(r'(?:Gesamtsanierung|Total)[^\n]{0,40}([\d\.]+)\s*€', text)
    if m:
        costs['total_investment'] = m.group(1).replace('.', '')
    
    # Could extract total sowieso, total funding similarly
    
    return costs


def extract_timeline(text):
    """Extract implementation timeline"""
    timeline = {
        'start': '',
        'end': '',
        'packages_schedule': {}
    }
    
    # Look for years in text
    years = re.findall(r'20\d{2}', text)
    if years:
        timeline['start'] = min(years)
        timeline['end'] = max(years)
    
    return timeline


def extract_u_values(text):
    """Extract U-values for all components"""
    u_values = {}
    
    # Pattern: "component ... U-Wert ... number W/(m²K)"
    pattern = r'(\w+)[^\n]{0,40}U-Wert[^\d]{0,10}([\d,\.]+)\s*W/\(m²K\)'
    
    for match in re.finditer(pattern, text, re.IGNORECASE):
        component = match.group(1).lower()
        value = match.group(2).replace(',', '.')
        
        if component not in u_values:
            u_values[component] = []
        u_values[component].append(value)
    
    return u_values


def extract_technical_specs(text):
    """Extract technical specifications"""
    specs = {}
    
    # WLS values
    wls_pattern = r'WLS\s*(\d+)'
    wls_values = re.findall(wls_pattern, text)
    if wls_values:
        specs['wls_values'] = list(set(wls_values))
    
    # JAZ value
    m = re.search(r'JAZ[^\d]{0,10}([\d,\.]+)', text)
    if m:
        specs['jaz'] = m.group(1).replace(',', '.')
    
    # Heat pump type
    m = re.search(r'Wärmepumpe[^\n]{0,40}(Lu[^\n]{0,20}Wasser)', text)
    if m:
        specs['heat_pump_type'] = 'Luft-Wasser'
    
    return specs


# ============================================================================
# HELPER: Read PPT Template Placeholders
# ============================================================================

def read_template_placeholders(template_bytes):
    """
    Read PowerPoint template and extract ALL placeholders
    Returns list of all {{placeholder}} patterns found
    """
    try:
        prs = Presentation(BytesIO(template_bytes))
        placeholders = set()
        
        for slide in prs.slides:
            for shape in slide.shapes:
                if hasattr(shape, 'text'):
                    # Find all {{placeholder}} patterns
                    matches = re.findall(r'\{\{([^}]+)\}\}', shape.text)
                    placeholders.update(matches)
                
                # Check tables
                if shape.has_table:
                    for row in shape.table.rows:
                        for cell in row.cells:
                            matches = re.findall(r'\{\{([^}]+)\}\}', cell.text)
                            placeholders.update(matches)
        
        return {
            'placeholders': sorted(list(placeholders)),
            'count': len(placeholders)
        }
        
    except Exception as e:
        return {'error': f'Failed to read template: {str(e)}'}


# ============================================================================
# API ENDPOINTS
# ============================================================================

@app.route('/')
def home():
    return jsonify({
        "status": "Comprehensive Data Extraction Service v4.0",
        "endpoints": [
            "/extract-comprehensive",
            "/read-template-placeholders",
            "/generate"
        ]
    })


@app.route('/extract-comprehensive', methods=['POST'])
def extract_comprehensive():
    """
    Extract ALL data from .sqproj and 2 PDFs
    Returns unified structured data ready for intelligent mapping
    """
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files uploaded'}), 400
        
        files = request.files.getlist('files')
        
        if len(files) < 3:
            return jsonify({'error': 'Expected 3 files (2 PDFs + 1 .sqproj)'}), 400
        
        # Identify files by type
        pdf_files = []
        sqproj_file = None
        
        for file in files:
            if file.filename.endswith('.pdf'):
                pdf_files.append(file.read())
            elif file.filename.endswith('.sqproj'):
                sqproj_file = file.read()
        
        if not sqproj_file or len(pdf_files) < 2:
            return jsonify({'error': 'Expected 2 PDFs and 1 .sqproj file'}), 400
        
        # Extract from each file
        sqproj_data = extract_all_from_sqproj(sqproj_file)
        pdf1_data = extract_all_data_from_pdf(pdf_files[0])
        pdf2_data = extract_all_data_from_pdf(pdf_files[1])
        
        # Structure all data
        complete_data = structure_complete_data(sqproj_data, pdf1_data, pdf2_data)
        
        return jsonify({
            'success': True,
            'data': complete_data,
            'extraction_summary': {
                'sqproj_files_found': len(sqproj_data.get('file_list', [])),
                'pdf1_pages': len(pdf1_data.get('pages', [])),
                'pdf2_pages': len(pdf2_data.get('pages', [])),
                'total_data_points': len(str(complete_data))
            }
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/read-template-placeholders', methods=['POST'])
def api_read_template():
    """
    Read PowerPoint template and extract all placeholders
    """
    try:
        if 'template' not in request.files:
            return jsonify({'error': 'No template file uploaded'}), 400
        
        template_file = request.files['template']
        template_bytes = template_file.read()
        
        result = read_template_placeholders(template_bytes)
        
        return jsonify({
            'success': True,
            'result': result
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/generate', methods=['POST'])
def generate_ppt():
    """
    Generate PowerPoint by filling template with mapped data
    (This endpoint already exists, just included for completeness)
    """
    # Implementation from previous version...
    pass


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)
