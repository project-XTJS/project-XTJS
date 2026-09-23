import unittest

from PIL import Image

from app.service.analysis.signature_presence import (
    _field_regions,
    _inline_signature_value,
    _suppress_red_seal,
    _value_region_left,
)


class SignaturePresenceFieldTests(unittest.TestCase):
    def test_inline_single_character_and_full_name_skip_image_detection(self):
        self.assertEqual(_inline_signature_value('法定代表人签字：张'), '张')
        self.assertEqual(_inline_signature_value('法定代表人签字：张三'), '张三')
        payload = {'bbox_coordinate_space': 'pdf_points', 'layout_sections': [
            {'page': 40, 'type': 'text', 'text': '法定代表人签字：张', 'bbox': [70, 529, 431, 552]},
            {'page': 41, 'type': 'text', 'text': '法定代表人签字：张三', 'bbox': [70, 529, 431, 552]},
        ]}
        self.assertEqual(_field_regions(payload), [])

    def test_blank_signature_field_is_selected_without_running_ocr(self):
        payload = {'bbox_coordinate_space': 'pdf_points', 'layout_sections': [{
            'page': 42, 'type': 'text',
            'text': '被授权人（签字或盖章）：___',
            'bbox': [85, 317, 334, 334],
        }]}
        fields = _field_regions(payload)
        self.assertEqual(len(fields), 1)
        self.assertEqual(fields[0]['page'], 42)
        self.assertEqual(fields[0]['field_box'], [85.0, 317.0, 334.0, 334.0])

    def test_printed_field_labels_and_dates_are_not_signature_values(self):
        self.assertIsNone(_inline_signature_value('法定代表人签字：___'))
        self.assertIsNone(_inline_signature_value('法定代表人签字：2026年9月14日'))
        self.assertIsNone(_inline_signature_value('法定代表人签字：签字'))

    def test_detector_crop_starts_after_printed_label(self):
        field = {
            'field_text': '被授权人（签字或盖章）：',
            'field_box': [85.0, 317.0, 334.0, 334.0],
        }
        self.assertGreater(_value_region_left(field), 200)
        self.assertIsNone(_value_region_left({
            'field_text': '法定代表人签名',
            'field_box': [100.0, 300.0, 220.0, 320.0],
        }))

    def test_red_company_seal_is_removed_without_erasing_black_ink(self):
        image = Image.new('RGB', (2, 1), 'white')
        image.putpixel((0, 0), (210, 30, 25))
        image.putpixel((1, 0), (20, 20, 20))
        cleaned = _suppress_red_seal(image)
        self.assertEqual(cleaned.getpixel((0, 0)), (255, 255, 255))
        self.assertEqual(cleaned.getpixel((1, 0)), (20, 20, 20))


if __name__ == '__main__':
    unittest.main()
