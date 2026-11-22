import io,re,zipfile
from xml.etree import ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
MAX_EXTRACTED=2_000_000
OFFICE={'.docx','.pptx','.xlsx','.epub'}
class HtmlText(HTMLParser):
 def __init__(self):super().__init__();self.parts=[];self.skip=0
 def handle_starttag(self,tag,attrs):
  if tag in {'script','style'}:self.skip+=1
  elif tag in {'p','div','br','h1','h2','li'}:self.parts.append('\n')
 def handle_endtag(self,tag):
  if tag in {'script','style'}:self.skip=max(0,self.skip-1)
 def handle_data(self,data):
  if not self.skip:self.parts.append(data)
def parse_container(filename,data):
 ext=Path(filename).suffix.lower()
 if ext=='.pdf':
  from pypdf import PdfReader
  reader=PdfReader(io.BytesIO(data));out=[]
  if len(reader.pages)>1000:raise ValueError('page_limit')
  for i,page in enumerate(reader.pages):
   out.append(f'# 第{i+1}页\n'+(page.extract_text() or ''))
   if sum(map(len,out))>MAX_EXTRACTED:raise ValueError('text_limit')
  if not any(x.split('\n',1)[-1].strip() for x in out):raise ValueError('ocr_required')
  return '\n\n'.join(out)
 with zipfile.ZipFile(io.BytesIO(data)) as z:
  infos=z.infolist()
  if len(infos)>10000 or sum(i.file_size for i in infos)>256*1024*1024:raise ValueError('container_limit')
  names=set(z.namelist());out=[]
  def xml(name):
   raw=z.read(name)
   if b'<!DOCTYPE' in raw.upper() or b'<!ENTITY' in raw.upper():raise ValueError('xml_entities')
   return ET.fromstring(raw)
  def strings(node):return ' '.join((n.text or '') for n in node.iter() if n.tag.rsplit('}',1)[-1] in {'t','v'})
  if ext=='.docx':out=[strings(xml('word/document.xml'))]
  elif ext=='.pptx':
   for name in sorted((n for n in names if re.fullmatch(r'ppt/slides/slide\d+.xml',n)),key=lambda n:int(re.search(r'(\d+)\.xml',n)[1])):out.append('# '+name+'\n'+strings(xml(name)))
  elif ext=='.xlsx':
   shared=[strings(n) for n in xml('xl/sharedStrings.xml')] if 'xl/sharedStrings.xml' in names else []
   for name in sorted(n for n in names if re.fullmatch(r'xl/worksheets/sheet\d+.xml',n)):
    lines=[]
    for row in xml(name).iter():
     if row.tag.rsplit('}',1)[-1]!='row':continue
     cells=[]
     for c in row:
      value=strings(c)
      if c.attrib.get('t')=='s' and value.isdigit() and int(value)<len(shared):value=shared[int(value)]
      cells.append(value)
     lines.append('\t'.join(cells))
    out.append('# '+name+'\n'+'\n'.join(lines))
  elif ext=='.epub':
   for name in sorted(n for n in names if n.lower().endswith(('.xhtml','.html','.htm'))):
    parser=HtmlText();parser.feed(z.read(name).decode('utf-8-sig'));out.append('# '+name+'\n'+''.join(parser.parts))
  text='\n\n'.join(out)
  if not text.strip() or len(text)>MAX_EXTRACTED:raise ValueError('empty_or_oversized_text')
  return text
