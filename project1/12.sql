select Pokemon.name from Pokemon join Evolution on Pokemon.id = Evolution.before_id where Evolution.before_id > Evolution.after_id order by Pokemon.name;
