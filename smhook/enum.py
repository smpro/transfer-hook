'''
Prevides enum representation, see
http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python

Example Usage:

    >>> Numbers = enum(ONE=1, TWO=2, THREE='three')
    >>> Numbers.ONE
    1
    >>> Numbers.TWO
    2
    >>> Numbers.THREE
    'three'
'''

def enum(*sequential, **named):
    '''
    Provides support for automatic enumeration. For example:
    
        >>> Numbers = enum('ZERO', 'ONE', 'TWO')
        >>> Numbers.ZERO
        0
        >>> Numbers.ONE
        1
        
    '''
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)
    
if __name__ == '__main__':
    import doctest
    doctest.testmod()
