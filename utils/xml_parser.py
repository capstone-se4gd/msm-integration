# utils/xml_parser.py
import lxml.etree as ET  # Use lxml instead of the standard library
import asyncio
from utils.helpers import fetch_url_data
from functools import lru_cache

@lru_cache(maxsize=128)
def xml_to_json(xml_content):
    """Convert Finvoice XML content to JSON with caching."""
    try:
        # Define the Finvoice namespace
        namespaces = {'fin': 'http://www.finvoice.fi/Finvoice'}

        # Parse the XML content - use faster lxml parser
        root = ET.fromstring(xml_content.encode('utf-8'))

        # Helper function to strip namespace
        def strip_namespace(tag):
            return tag.split('}', 1)[1] if '}' in tag else tag

        # Recursive function to process elements
        def process_element(element):
            data = {}
            for child in element:
                tag = strip_namespace(child.tag)
                if list(child):
                    data[tag] = process_element(child)
                else:
                    data[tag] = child.text.strip() if child.text else ''
            return data

        result = {}
        for child in root:
            tag = strip_namespace(child.tag)
            if tag == 'InvoiceRow':
                # Process all InvoiceRow elements
                invoice_rows = []
                for invoice_row in root.findall('fin:InvoiceRow', namespaces):
                    row_data = {}
                    for elem in invoice_row:
                        elem_tag = strip_namespace(elem.tag)
                        if elem_tag == 'SpecificationDetails':
                            # Extract SpecificationFreeText
                            spec_texts = [spec.text.strip() for spec in elem.findall('fin:SpecificationFreeText', namespaces) if spec.text]
                            row_data['SpecificationDetails'] = spec_texts
                        elif elem_tag == 'Other':
                            result['other_url'] = elem.text.strip() if elem.text else ''
                            row_data['Other'] = elem.text.strip() if elem.text else ''
                        else:
                            row_data[elem_tag] = elem.text.strip() if elem.text else ''
                    invoice_rows.append(row_data)
                result['InvoiceRows'] = invoice_rows
            else:
                result[tag] = process_element(child)

        return result
    except Exception as e:
        return {'error': f'XML parsing error: {str(e)}'}


async def process_xml_file(xml_content):
    """Process a single XML file."""
    try:
        # Convert XML to JSON
        json_data = xml_to_json(xml_content)
        
        # If there's an 'other_url', fetch data from it
        if 'other_url' in json_data and 'error' not in json_data:
            sustainability_data = await fetch_url_data(json_data['other_url'])
            
            # Add sustainability metrics to JSON data
            if 'error' not in sustainability_data:
                # First extract sustainability_metrics from the response if available
                if 'sustainability_metrics' in sustainability_data:
                    json_data['sustainabilityMetrics'] = sustainability_data['sustainability_metrics']
                else:
                    # If no sustainability metrics, add error
                    json_data['sustainabilityMetrics'] = {'error': 'No sustainability metrics found'}
            else:
                json_data['sustainabilityMetrics'] = {'error': sustainability_data['error']}
        
        return json_data
    except Exception as e:
        return {'error': f'Processing error: {str(e)}'}


# Process multiple XML files
async def process_multiple_xml_files(xml_files):
    """Process multiple XML files concurrently with optimized batching."""
    # Process in batches to avoid flooding resources
    BATCH_SIZE = 5
    all_results = []
    
    for i in range(0, len(xml_files), BATCH_SIZE):
        batch = xml_files[i:i+BATCH_SIZE]
        tasks = [process_xml_file(content) for content in batch]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)
    
    return all_results

