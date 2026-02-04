# ocr_simple.py
import pytesseract
import cv2
import fitz  # PyMuPDF
import os
import tempfile
import pandas as pd  
from PIL import Image
import io


def extract_table_advanced(image_path):
    # Загружаем изображение
    img = cv2.imread(image_path)
    
    # Преобразуем в PIL Image
    from PIL import Image
    pil_img = Image.fromarray(cv2.cvtColor(img, cv2.COLOR_BGR2RGB))
    
    # Используем Tesseract для получения структурированных данных
    data = pytesseract.image_to_data(
        pil_img,
        lang='rus',
        output_type=pytesseract.Output.DATAFRAME,
        config='--psm 11'  # PSM 11 - разреженный текст
    )
    
    # Очищаем данные
    data = data[data['conf'] > 30]
    data = data[data['text'].notna()]
    data['text'] = data['text'].apply(lambda x: str(x).strip())
    data = data[data['text'] != '']
    
    # Определяем строки и колонки
    data['row_group'] = pd.cut(data['top'], bins=20, labels=range(20))
    data['col_group'] = pd.cut(data['left'], bins=10, labels=range(10))
    
    # Создаем таблицу
    table = data.pivot_table(
        index='row_group',
        columns='col_group',
        values='text',
        aggfunc=lambda x: ' '.join(x),
        fill_value=''
    )
    
    # Очищаем названия колонок
    table = table.reset_index()
    table.columns = [f'Column_{i}' for i in range(len(table.columns))]
    
    return table

def extract_text_from_pdf(pdf_path):
    """Извлекает текст из PDF файла"""
    if not os.path.exists(pdf_path):
        print(f"Файл {pdf_path} не найден!")
        return
    
    print(f"Обрабатываю {pdf_path}...")
    
    doc = fitz.open(pdf_path)
    all_text = ""
    
    for page_num in range(len(doc)):
        print(f"  Страница {page_num + 1} из {len(doc)}...")
        
        page = doc[page_num]
        # Конвертируем в изображение
        pix = page.get_pixmap(dpi=150)  # 150 DPI для качества
        
        # Конвертируем в PIL Image
        
        img_bytes = pix.tobytes("png")
        image = Image.open(io.BytesIO(img_bytes))
        
        # with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
            # image.save(tmp_file, format='PNG')
            # temp_path = tmp_file.name

        # res =  extract_table_advanced(temp_path)
        # res.to_excel('output_table.xlsx', index=False, encoding='utf-8-sig')
        # Распознаем текст
        text = pytesseract.image_to_string(image, lang='rus')
        all_text += f"\n=== Страница {page_num + 1} ===\n{text}\n"
    
    doc.close()
    return all_text

# Основная часть
if __name__ == "__main__":
    # Укажите путь к файлу
    pdf_file = "estimate.pdf"
    
    # Укажите путь к Tesseract если нужно
    # pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    
    if os.path.exists(pdf_file):
        text = extract_text_from_pdf(pdf_file)
        
        # Сохраняем результат
        with open("распознанный_текст.txt", "w", encoding="utf-8") as f:
            f.write(text)
        
        print("\n✅ Готово! Текст сохранен в 'распознанный_текст.txt'")
        print(f"📊 Всего символов: {len(text)}")
        
        # Показываем первые 500 символов
        print("\n📝 Первые 500 символов:")
        print("-" * 50)
        print(text)
        print("-" * 50)
    else:
        print(f"❌ Файл {pdf_file} не найден в текущей папке!")
        print("Положите файл estimate.pdf в ту же папку, где лежит скрипт.")


        import io
import pytesseract
from PIL import Image
import pandas as pd
import cv2
import numpy as np

# Загружаем изображение
image = Image.open(io.BytesIO(img_bytes))
img_array = np.array(image)

# Преобразуем в оттенки серого
gray = cv2.cvtColor(img_array, cv2.COLOR_RGB2GRAY)

# Применяем threshold
thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

# Находим контуры таблицы
contours = cv2.findContours(thresh, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
contours = contours[0] if len(contours) == 2 else contours[1]

# Распознаем текст с таблицей
custom_config = r'--oem 3 --psm 6'
text = pytesseract.image_to_string(gray, config=custom_config, lang='rus+eng')

# Разделяем на строки и столбцы
rows = text.strip().split('\n')
table_data = []

for row in rows:
    # Предполагаем разделение табуляцией или пробелами
    cells = [cell.strip() for cell in row.split('\t') if cell.strip()]
    if cells:
        table_data.append(cells)

# Создаем DataFrame
if table_data:
    df = pd.DataFrame(table_data[1:], columns=table_data[0] if len(table_data) > 1 else None)
else:
    df = pd.DataFrame({'Extracted Text': [text]})

# Сохраняем в Excel
df.to_excel('table_from_image.xlsx', index=False)
print("Таблица сохранена в table_from_image.xlsx")

