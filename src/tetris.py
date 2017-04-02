import pygame
pygame.init()

screen = pygame.display.set_mode([800,1000])

BLOCK_WIDTH = 100
BLOCK_HEIGHT = 100
BLACK = (0,0,0)

DROP_EVENT = pygame.USEREVENT + 1


class Block():
    def __init__(self, image_path):
        self.image = pygame.image.load(image_path)
        self.x = 0
        self.y = 0

    def move_left(self):
        if self.x > 0:
            self.x -= BLOCK_WIDTH
            if self.x < 0:
                self.x = 0

    def move_right(self):
        if self.x < screen.get_width() - BLOCK_WIDTH:
            self.x += BLOCK_WIDTH
            if self.x > screen.get_width() - BLOCK_WIDTH:
                self.x = screen.get_width() - BLOCK_WIDTH

    def move_down(self):
        if self.y < screen.get_height() - BLOCK_HEIGHT:
            self.y += BLOCK_HEIGHT
            if self.y > screen.get_height() - BLOCK_HEIGHT:
                self.y = screen.get_height() - BLOCK_HEIGHT

    def rotate(self):
        self.image = pygame.transform.rotate( self.image, 90 )

    def position(self):
        return (self.x, self.y)

block = Block("/home/ctd/Desktop/stooge.png")

pygame.time.set_timer( DROP_EVENT, 1000 )

running = True
while running:
    for event in pygame.event.get():
        if event.type == pygame.QUIT:
            running = False
        elif event.type == pygame.KEYDOWN:
            if event.key == pygame.K_LEFT:
                block.move_left()
            elif event.key == pygame.K_RIGHT:
                block.move_right()
            elif event.key == pygame.K_UP:
                block.rotate()
        elif event.type == DROP_EVENT:
            block.move_down()
            
    # draw
    screen.fill(BLACK)
    screen.blit( block.image, block.position() )
    pygame.display.update()

pygame.quit()

                
            
def new_grid(x, y):
    grid = []                # empty grid (no rows)
    for i in range(y):
        row = []             # new row
        for j in range(x):
            row.append( 0 )  # add columns to row
        grid.append( row )   # add row to grid
    return grid


                
