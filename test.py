import PyPDF2

with open('pdf_page.pdf', 'rb') as pdfFileObj:
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    print(pdfReader.numPages)
    print(pdfReader.documentInfo)
    print(pdfReader.namedDestinations)
    print(pdfReader.pageLayout)
    print(pdfReader.pageMode)
    print(pdfReader.pages)
    print(pdfReader.xmpMetadata)

    pageObj = pdfReader.getPage(0)
    print(pageObj.extractText())


# Ref
# https://automatetheboringstuff.com/chapter13/
# https://aws.amazon.com/textract/

"""
 |  documentInfo
 |  
 |  isEncrypted
 |  
 |  namedDestinations
 |  
 |  numPages
 |  
 |  outlines
 |  
 |  pageLayout
 |      Get the page layout.
 |      See :meth:`setPageLayout()<PdfFileWriter.setPageLayout>`
 |      for a description of valid layouts.
 |      
 |      :return: Page layout currently being used.
 |      :rtype: ``str``, ``None`` if not specified
 |  
 |  pageMode
 |      Get the page mode.
 |      See :meth:`setPageMode()<PdfFileWriter.setPageMode>`
 |      for a description of valid modes.
 |      
 |      :return: Page mode currently being used.
 |      :rtype: ``str``, ``None`` if not specified
 |  
 |  pages
 |  
 |  xmpMetadata
"""

