import pytesseract
import cv2
import pandas as pd

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
    # добавляю комментарий ))
    
    # Очищаем названия колонок
    table = table.reset_index()
    table.columns = [f'Column_{i}' for i in range(len(table.columns))]
    
    return table

# Использование
result_table = extract_table_advanced('table.png')
print(result_table)

# Сохраняем в Excel
result_table.to_excel('output_table.xlsx', index=False, encoding='utf-8-sig')