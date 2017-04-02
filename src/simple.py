# 1
def maximum(x, y):
    if x > y:
        return x
    else:
        return y

print(maximum(10,20))
print(maximum(20,10))
print(maximum(10,10))
print("------")

# 2
def mo3(x, y, z):
    return maximum(x, maximum(y, z))

print(mo3(10, 20, 30))
print(mo3(10, 30, 20))
print(mo3(20, 10, 30))
print(mo3(20, 30, 10))
print(mo3(30, 10, 20))
print(mo3(30, 20, 10))
print("------")

# 3
def length(string):
    length = 0
    for char in string:
        length += 1
    return length

print(length("fred"), len("fred"))
print("------")

# 4
def is_vowel(char):
    for v in ["a", "e", "i", "o", "u"]:
        if (v == char.lower()):
            return True
    return False

print(is_vowel("a"), is_vowel("A"))
print(is_vowel("e"), is_vowel("E"))
print(is_vowel("i"), is_vowel("I"))
print(is_vowel("o"), is_vowel("O"))
print(is_vowel("u"), is_vowel("U"))
print(is_vowel("b"), is_vowel("B"))
print(is_vowel("fred"), is_vowel("FRED"))
print("------")

      
# 5
def is_consonant(char):
    return not is_vowel(char) # re-use is_vowel() from #4!

def translate(string):
    translation = ""
    for char in string:
        if ( is_consonant(char) ):
            translation += char + "o" + char
        else:
            translation += char
    return translation

print("fred", translate("fred"))
print("hello", translate("hello"))
print("------")

# 6
def sum(numbers):
    result = 0
    for number in numbers:
        result += number
    return result

def multiply(numbers):
    result = 1
    for number in numbers:
        result *= number
    return result

print(sum([1,2,3,4,5]), multiply([1,2,3,4,5]))
print(sum([0,1,2,3,4,5]), multiply([0,1,2,3,4,5]))
print("------")

# 7
def reverse(string):
    result = ""
    for i in reversed(string):
        result += i
    return result

print(reverse("fred"))
print("------")

# 8 
def is_palindrome(string):
    return string == reverse(string)  # re-use reverse() from #7!

print(is_palindrome("fred"))
print(is_palindrome("bob"))
print(is_palindrome("dood"))
print("------")


def collide(m1, posx, posy, m2):
    for y in range(len(m1)):
        row = m1[y]
        for x in range(len(row)):
            cell = row[x]
            board_cell = m2[posy+y][posx+x]
            #print(x,y,"cell =", cell,"|", x+posx, y+posy, "board cell =", board_cell)
            if cell == 1 and m2[y+posy][x+posx] == 1:
                return True
    return False

piece = [[1,0],
         [1,1],
         [0,1]]
board = [[0,0,0,0,0],
         [0,0,0,0,0],
         [0,0,0,1,0],
         [0,0,1,1,0],
         [0,0,0,1,0],
         [1,0,0,1,1]]

height = len(board)
width = len(board[0])
print("height", height)
print("width", width)


print(collide(piece, 0, 0, board))
print(collide(piece, 1, 0, board))
print(collide(piece, 2, 0, board))
print(collide(piece, 3, 0, board))
print()
print(collide(piece, 0, 1, board))
print(collide(piece, 1, 1, board))
print(collide(piece, 2, 1, board))
print(collide(piece, 3, 1, board))
print()        
print(collide(piece, 0, 2, board))
print(collide(piece, 1, 2, board))
print(collide(piece, 2, 2, board))
print(collide(piece, 3, 2, board))
print()          
print(collide(piece, 0, 3, board))
print(collide(piece, 1, 3, board))
print(collide(piece, 2, 3, board))
print(collide(piece, 3, 3, board))
print()
#print(collide(piece, 0, 4, board))
#print(collide(piece, 1, 4, board))
#print(collide(piece, 2, 4, board))
#print(collide(piece, 3, 4, board))
            
import numpy

    
                         
    

