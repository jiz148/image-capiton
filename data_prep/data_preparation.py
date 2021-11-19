import pandas as pd


def data_prep():
    df = pd.read_csv('data/wikipedia-image-caption/train-00000-of-00005.tsv',
                     encoding='utf8',
                     delimiter='\t',
                     nrows=10000000
                     )
    # filter english
    df = df[df['language'] == 'en']

    # filter columns
    df = df.filter(items=['image_url',
                          'caption_reference_description',
                          'caption_attribution_description',
                          'caption_alt_text_description']
                   )

    df = df.replace(to_replace='[\r\n]', value=' ', regex=True)

    df.to_csv('data/preprocessed_data.tsv', sep='\t', index=False)
    return df.describe()


print(data_prep())
