from os import listdir

def _paragraphs(book):
    # return list of paragraphs
    with open(book, 'r') as f:
        text = f.read()
        paras = text.split('\r\n\r\n') # list of paragraphs

        # remove extra new lines
        for i, para in enumerate(paras):
            paras[i] = ' '.join(para.split())

    return paras


def _wordcount(paragraph):
    # paragraph word count
    return len(paragraph.split())


def _sectionize(paragraph_list, min_wc=200):
    # return multi-paragraph sections with minimum word count = min_wc
    sections = [paragraph_list[0]]
    for paragraph in paragraph_list[1:]:
        if _wordcount(sections[-1]) < min_wc:
            sections[-1] += ' || ' + paragraph
        else:
            sections.append(paragraph)

    if _wordcount(sections[-1]) < min_wc:
        temp = sections.pop()
        sections[-1] += ' || ' + temp

    return sections


def process_book(book, min_wc = 200):
    # return list of dictionaries containing book title, author and a section of text

    result = []

    author = book.split('_')[1][:-4]
    title = book.split('_')[0].split('/')[-1]

    paragraphs = _paragraphs(book)
    sections = _sectionize(paragraphs, min_wc=min_wc)

    for sec in sections:
        ddd = {}
        ddd['author'] = author
        ddd['title'] = title
        ddd['text_blob'] = sec
        result.append(ddd)

    return result
