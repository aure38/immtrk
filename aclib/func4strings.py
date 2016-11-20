# coding: utf8
import unicodedata
import string
import html
import re

class Func4strings:
    def __init__(self):
        super().__init__()

    @staticmethod
    def strCleanSanitize(input_str,
                         phtmlunescape=True,            # Enlever tout ce qui est entre des tags HTML + Unescape les &...;
                         pLignesTabsGuillemets=True,    # Enlever tout les tabs, retours a la ligne, guillemets et apotrosphes a l'envers remplaces par apostrophe normale
                         pNormalizeASCII=True,          # Enlever les accents, euro en E, livre, section... garde $ # & @ normalisation NFKD ET Ascii et les caracteres
                         pEnleveSignesSpeciaux=False,   # On ne garde que -.'+#?!%&*/()=_:;, et lettres digits en faisant quelques transfo : [ en ( ...
                         pLettreDigitPointTiret=False,  # One ne garde que -. 0123456789 abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
                         pLetterDigitTiretOnly=False,    # lettres chiffres train d'union
                         pBagOfWords=False):            # lettres et digits en lowercase
        retour = ""
        if input_str is not None and len(input_str) > 0 :
            retour = input_str
            if phtmlunescape :
                retour = re.sub(u"<.*?>", " ", input_str)   # On enleve les balises HTML
                retour = html.unescape(retour)              # on remet les caract normaux

            if pLignesTabsGuillemets :
                retour = retour.replace("""\\n""", """\n""").replace("""\\'""", """'""")
                retour = Func4strings.strMultiReplace([(r"\'","'"), ('"', "'"), ("’", "'"), ("`", "'"), ('\n', ' '), ('\r', ''), ('\t', ' ')], retour)

            if pNormalizeASCII :   # On fait sauter les accents en les transformant en lettre equivalente
                retour = Func4strings.strMultiReplace([('€','EUROS'), ('£','POUNDS'), ('°','DEG')], retour)
                retour = (unicodedata.normalize('NFKD', retour).encode('ascii', 'ignore')).decode('utf-8','ignore')
            else: # UTF-8 : on garde plus de trucs
                retour = (retour.encode('utf-8', 'ignore')).decode('utf-8','ignore')  #retour = (unicodedata.normalize('NFKD', retour).encode('utf-8', 'ignore')).decode('utf-8','ignore')

            # --- LES SUIVANTS SE CUMULENT
            if pEnleveSignesSpeciaux :
                trans = str.maketrans("²\\[]{}<>^|~", "2/()()     ")
                retour = retour.translate(trans)
                retour.maketrans("","")
                validFilenameChars="0123456789 abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-.'+#?!%&*/()=_:;,"
                retour = "".join([c for c in retour if c in validFilenameChars])

            if pLettreDigitPointTiret :
                trans = str.maketrans("²\\[]{}<>^|~!?'%#&/*+:;_()","2/()()     ..            ")
                retour = retour.translate(trans)
                retour.maketrans("","")
                validFilenameChars="0123456789 abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-."
                retour = "".join([c for c in retour if c in validFilenameChars])

            if pLetterDigitTiretOnly :
                trans = str.maketrans("²\\[]{}<>^|~!?'%#&/*+:;_().","2                         ")
                retour = retour.translate(trans)
                retour.maketrans("","")
                validFilenameChars="0123456789 abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-"
                retour = "".join([c for c in retour if c in validFilenameChars])

            if pBagOfWords :
                trans = str.maketrans("²\\[]{}<>^|~!?'%#&/*+:;_().-","2                          ")
                retour = retour.translate(trans)
                retour.maketrans("","")
                validFilenameChars = " %s%s" % (string.ascii_letters, string.digits)
                retour = "".join([c for c in retour if c in validFilenameChars])
                retour = retour.lower()

            retour = re.sub(u" +", " ", retour).strip() # On enleve les espaces en trop
        return retour


    @staticmethod
    def cleanLangueFr(input_str) :
        return Func4strings.strCleanSanitize(input_str, phtmlunescape=True, pLignesTabsGuillemets=True,
                                             pNormalizeASCII=False,
                                             pEnleveSignesSpeciaux=False, pLettreDigitPointTiret=False,
                                             pLetterDigitTiretOnly=False, pBagOfWords=False)
    @staticmethod
    def cleanOnlyLetterDigit(input_str) :
        return Func4strings.strCleanSanitize(input_str, phtmlunescape=True, pLignesTabsGuillemets=True,
                                             pNormalizeASCII=True,
                                             pEnleveSignesSpeciaux=False, pLettreDigitPointTiret=False,
                                             pLetterDigitTiretOnly=True, pBagOfWords=False)
    @staticmethod
    def cleanMax(input_str) :
        return Func4strings.strCleanSanitize(input_str, phtmlunescape=True, pLignesTabsGuillemets=True,
                                             pNormalizeASCII=True,
                                             pEnleveSignesSpeciaux=False, pLettreDigitPointTiret=False,
                                             pLetterDigitTiretOnly=False, pBagOfWords=True)
    @staticmethod
    def strMultiReplace(subs, subject) :
        """Simultaneously perform all substitutions on the subject string.
        multisub([('hi', 'bye'), ('bye', 'hi')], 'hi and bye')
        -> 'bye and hi'

        pour du unicaractere :
        import string
        trans = string.maketrans("ABCDE","12345")
        my_string = my_string.translate(trans)

        """
        pattern = '|'.join('(%s)' % re.escape(p) for p, s in subs)
        substs = [s for p, s in subs]
        replace = lambda m: substs[m.lastindex - 1]
        return re.sub(pattern, replace, subject)
    @staticmethod
    def strMatchAny(p_patterns, p_original_text) :
        """if strMatchMultiple(['toto','tata','titi'], "blablabla") ... Return True or False
        """
        match_test = [True for match in p_patterns if match in p_original_text]
        return True in match_test

