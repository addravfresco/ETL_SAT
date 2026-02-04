"""
REPOSITORIO MAESTRO DE REGLAS DE NORMALIZACIÓN (CLEANING RULES)
Contiene la base de conocimiento para la reparación de Mojibake, estandarización 
de regímenes fiscales, poda de basura geográfica y validación de sintaxis RFC.
"""

# ==============================================================================
# 1. MOTOR DE REPARACIÓN DE MOJIBAKE (Encoding Recovery)
# ==============================================================================


"""
REPOSITORIO MAESTRO DE REGLAS DE NORMALIZACIÓN (CLEANING RULES)
Contiene la base de conocimiento para la reparación de Mojibake.
Actualizado: Fase 3 Big Data (Eth + Lossy Rombos).
"""

REEMPLAZOS_MOJIBAKE = {
    'A¯ ¿ ¿AGA': 'ÑAGA',
    'A¯ ¿ ¿IGA': 'ÑIGA',
    'A¯ ¿ ¿UELOS': 'ÑUELOS',
    'A¯ ¿ ¿A': 'ÑA',
    'A¯ ¿ ¿O': 'ÑO',
    'A¯ ¿ ¿EZ': 'ÑEZ',
    'A¯ ¿ ¿OS': 'ÑOS',
    'A¯ ¿ ¿': 'Ñ', 

    # --- Casos Críticos: Triple Encoding & Corrupción Profunda ---
    'ÃƒÂƒÃ¢Â€Â˜': 'Ñ', 
    'ÃƒÂƒÃ¢Â€Â': 'Ñ', 
    'Ã¯Â¿Â½': 'Ñ', 
    'A¯ ¿ ½': 'Ñ',
    'DÂ¥': 'D', 
    'D ¥': 'D',
    'â€¦': '', 
    ' ¦': '', 
    'Â·': '',

    # --- Alta Prioridad: Errores de Lectura Comunes ---
    'ARÑR': 'O', 
    'CIA “N': 'CION', 
    'CIA“N': 'CION', 
    '“N': 'ON', 
    '“': 'O', 
    'A “': 'O',

    # --- Double Encoding (UTF-8 as Latin-1) ---
    'ÃƒÂ±': 'Ñ', 
    'ÃƒÂ‘': 'Ñ', 
    'Ãƒ ': 'Ñ', 
    'Ãƒ?': 'Ñ', 
    'Ãƒ': 'Ñ', 
    'Ã‰': 'E', 
    'Ã“': 'O', 
    'Ã”': 'O', 
    'Ã…': 'A', 
    'Ã‘': 'Ñ', 
    'Ã±': 'Ñ', 
    'Ã¡': 'A', 
    'Ã©': 'E', 
    'Ã­': 'I', 
    'Ã³': 'O', 
    'Ãº': 'U', 
    'Ãš': 'U',
    'Ã‡': ' ', 
    'ÃŒ': 'U', 
    'Ã.': 'Ñ', 
    'A‰': 'E',
    'A“': 'O', 
    'A Œ': 'O',
    'A¨': 'E', 
    'A©': 'E', 
    'A ©': 'E', 
    '©': 'E',
    'Ã\x8d': 'I', 
    'Ã\xad': 'I', 
    'Ã\x81': 'A', 
    'AGUIAƑ˜': 'Ñ', 
    'MARÃA': 'MARIA', 
    'ALCALDÃA': 'ALCALDIA', 
    'GARCÃA': 'GARCIA',

    # --- Hexadecimales, Símbolos y Residuos de Parsing ---
    '¥': 'Ñ',
    'Ã¥': 'Ñ',
    'Â¥': 'Ñ',
    'ÃA': 'IA', 
    'Ƒ˜': 'Ñ', 
    '˜': '', 
    'Ƒ': '', 
    '¨': '',
    '’': '', 
    '‘': '', 
    '´': '', 
    '²': 'O', 
    '¹': 'U', 
    '™': '', 
    '¬': 'I', 
    'Œ': 'O', 
    '±': 'Ñ', 
    'A ±': 'Ñ', 
    'A O': 'Ñ', 
    'A ‰': 'E',
    'Ï¿½': '', 
    '°': '', 
    'º': '', 
    '§': '', 
    '¼': 'U',

    # --- Patrones de Incertidumbre e Interrogantes ---
    'Ã ': 'A', 
    'Ã': 'A', 
    'GARC?A': 'GARCIA', 
    'GARC A': 'GARCIA', 
    'G?MEZ': 'GOMEZ', 
    'G MEZ': 'GOMEZ',
    'P?REZ': 'PEREZ', 
    ' P REZ': ' PEREZ', 
    'ORDO?EZ': 'ORDOÑEZ', 
    'ORDO EZ': 'ORDOÑEZ', 
    'LUC?A': 'LUCIA', 
    'MAR?A': 'MARIA', 
    'D?AZ': 'DIAZ', 
    'MU?OZ': 'MUÑOZ', 
    'MU EZ': 'MUÑOZ', 
    'BA?OS': 'BAÑOS', 
    'Í': 'I',

    # --- Entidades Especiales y Varios ---
    '&AMP;': '&', 
    '&QUOT;': '', 
    'Â®': '', 
    'Â': ' ', 
    '€': '', 
    'Âº': '', 
    '¡': 'I', 
    '': ' ', 
    '‹': '', 
    '›': '',

    # ==============================================================================
    # --- NUEVOS HALLAZGOS FASE 3 (BIG DATA & UTF8-LOSSY) ---
    # ==============================================================================
    # 1. Correcciones de Palabras Completas (Prioridad Alta)
    'BAOS': 'BAÑOS',
    'SALDAA': 'SALDAÑA',
    'COMPAIA': 'COMPAÑIA',
    'NIO': 'NIÑO',
    'DISEO': 'DISEÑO',
    'AO': 'AÑO',
    
    # 2. Caracteres "Basura" detectados en SQL y Profiler
    'Ð': 'Ñ',   # Eth Mayúscula (Error común por Ñ)
    'ð': 'ñ',   # Eth Minúscula
    '\ufffd': 'Ñ'    # El "Rombo" generado por utf8-lossy (Asumimos Ñ por contexto)
}

# ==============================================================================
# 2. IDENTIFICACIÓN DE REGÍMENES FISCALES (Legal Entities)
# ==============================================================================
# Regex optimizados para detectar y segmentar la denominación social.

REGIMENES_FISCALES = {
    "S.P.R. DE R.L. DE C.V.": [r"S\.?P\.?R\.? ?DE ?R\.?[LI]\.? ?DE ?C\.? ?V\.?", r"(?<=\s)SPR DE RL DE CV"],
    "S.C. DE P. DE R.L. DE C.V.": [r"S\.?C\.? ?DE ?P\.? ?DE ?R\.?L\.? ?DE ?C\.? ?V\.?"],
    "S.P.R. DE R.L.": [r"S\.?P\.?R\.? ?DE ?R\.?[LI]\.?", r"(?<=\s)SPR DE RL"],
    "S.A.P.I. DE C.V.": [r"S\.? ?A\.? ?P\.? ?I\.? ?DE ?C\.? ?V\.?", r"(?<=\s)SAPI DE CV"],
    "S.A. DE C.V.": [r"S\.? ?A\.? ?DE ?C\.? ?V\.?", r"S\.?A\.? ?C\.?V\.?", r"\.C\.V\."],
    "S. DE R.L. DE C.V.": [r"S\.? ?DE ?R\.? ?L\.? ?DE ?C\.? ?V\.?", r"(?<=\s)SRL DE CV"],
    "S.A.S. DE C.V.": [r"S\.?A\.?S\.?\s?DE\s?C\.?\s?V\.?", r"(?<=\s)SAS DE CV"],
    "S.A.S.": [r"S\.?A\.?S\.?$", r"(?<=\s)SAS$"],
    "I.A.P.": [r"I\.A\.P\.", r"(?<=\s)I ?A ?P$", r"^I ?A ?P$"], 
    "S.C.L.": [r"S\.?C\.?L\.?$", r"(?<=\s)SCL$"],
    "S.C.": [r"\bS\.? ?C\.?$", r"(?<=[A-Z])\.S\.C\.", r"(?<=\s)SC$"], 
    "A.C.": [r"\bA\.? ?C\.?$", r"(?<=\s)AC$"], 
    "S.A.": [r"\bS\.? ?A\.?$", r"(?<=\s)SA$"] 
}

# ==============================================================================
# 3. PATRONES DE PODA ESTRUCTURAL (Data Scrubbing)
# ==============================================================================
# Reglas de limpieza para eliminar residuos de domicilios y metadatos técnicos.

PATRONES_PODA = [
    # --- 1. Referencias Geográficas y Metadata Absoluta ---
    r'//:.*',
    r'\bALCALD[IÃA]+A\b.*',
    r'\bVILLA\s+OCAMPO.*',
    r'\bDOMICILIO\s+CONOCIDO.*',
    r'\bMUNICIPIO\s+DE\s+.*',
    r'\bLOC\.?\s?.*$',
    r'\bDIRECCION\b.*',
    r'\bDIR\.?\s+[A-Z].*',
    
    # --- 2. Vías con Blindaje Numérico ---
    r'\b(CALLE|CALLEJON|AVENIDA|CALZADA|AV\.?|BOULEVARD|CALLE:)\s?#?\s?\d+.*',
    r'\b(CALLE|CALLEJON|AVENIDA|CALZADA|AV\.?|BOULEVARD)\s+[A-Z]+\s#?\s?\d+.*',
    
    # --- 3. Estructura de Altura y Dirección ---
    r'\s#?\s?\d+\s?\.?\s?(COLONIA|COL|ALCALD|MUNICIPIO|C\.?P\.?|YURIRIA|GUANAJUATO|MEXICO).*', 
    
    # --- 4. Colonias y Catastro ---
    r'\b(COLONIA|COL\.?)\s?\d+.*', 
    r'\b(COLONIA|COL\.?)\b\s[A-Z\s]+\d+.*', 
    r'\s(MZ|LT|MZ\.?|LT\.?)\s?\d+.*', 
    r'\b(SN|LOCAL)\b.*',
    
    # --- 5. Estructuras Genéricas ---
    r'\s[A-ZÑ\s]+\s\d+\s?[-/]?\s?[A-Z]?\s+(COLONIA|COL|C\.?P\.?).*',
    
    # --- 6. Identificadores de Domicilio ---
    r'\b(NUM\.?|EXT\.?|INT\.?)\s*(?:NUM\.?|EXT\.?|INT\.?|N\.?)*\s?#?\s?\d+.*',
    r'\bN\.?\s?#?\s?\d+\b.*',
    
    # --- 7. Metadata Técnica ---
    r'\bC\.?P\.?\s?\d{5}.*', 
    r'\bCED\.?\s?PROF\.?\s?\d+.*',
    r'\bRFC\s?[A-Z0-9]+.*',
    r'\b(FOLIO|FAC|FACTURA|ID|PROVEEDOR)\s*[:#]?\s*\d+.*'
]

# ==============================================================================
# 4. NORMALIZACIÓN DE IDENTIDAD FISCAL (RFC Cleaning)
# ==============================================================================
# Diccionario fusionado para la recuperación de la cadena del RFC.

REEMPLAZOS_RFC = {
    'Ã“': 'O', 'Ã”': 'O', 'Ã…': 'A', 'Ã‘': 'Ñ', 'Ã±': 'Ñ', 'Ã\x91': 'Ñ', 
    'Ã\x81': 'A', 'Ã¡': 'A', 'Ã\x83': 'A', 'ÃCx81': 'A', 'A': 'A',
    'Ã\x89': 'E', 'Ã©': 'E', 'ÃCx89': 'E', 'A‰': 'E', 'A©': 'E',
    'Ã\x8d': 'I', 'Ã­': 'I', 'ÃCx8D': 'I', 'A­': 'I', 'A': 'I',
    'Ã\x93': 'O', 'Ã³': 'O', 'ÃCx93': 'O', 'A³': 'O', 'A“': 'O',
    'Ã\x9a': 'U', 'Ãº': 'U', 'ÃŠ': 'U', 'Ãš': 'U', 'Aº': 'U',
    'A‰': 'E', 'A“': 'O', 
    'AƒÂ±': 'Ñ', 'AÂ¥': 'Ñ', 'A¯Â¿Â½': 'Ñ', 
    'AƒÂ­': 'I', 'AƒÂ³': 'O', 'AƒÂº': 'U', 'AÂ´': "'", 'Â´': "'",
    'Â®': '', 'Â': '', '€': '', 'Â€U': '', 'Âº': '', '&AMP;': '&', 
    '¡': 'I', '“': 'O', '': '', 
    '&QUOT;': '', 'â€œ': '', 'â€': ''
}