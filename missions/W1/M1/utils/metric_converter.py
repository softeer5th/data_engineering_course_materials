def mile_to_km(mile):
    return mile * 1.60934

def gallon_to_liter(gallon):
    return gallon * 3.78541

def mpg_to_km_per_liter(mpg):
    return mpg * 0.425143  # mpg * mile_to_km(1) / gallon_to_liter(1) 

def inch_to_cc(inch):
    return inch * 2.54

def cubic_inch_to_cc(cubic_inch):
    return cubic_inch * 16.3871 # cubic_inch * inch_to_cc(1) ** 3

def pound_to_kg(pound):
    return pound * 0.453592

def pound1000_to_ton(pound1000):
    return pound1000 * 0.453592