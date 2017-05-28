

    def move_left(self, board):
        self.x -= 1
        if is_colliding(
        if self.x > 0:
            self.x -= 1


    def is_colliding(self, board):
        for y in range( PIECE_SIZE ):
            for x in range( PIECE_SIZE ):
                cell = self.grid[y][x]
                board_cell = board[self.y + y][self.x + x]
                if cell > -1 and board_cell > -1:
                    return True
        return False
