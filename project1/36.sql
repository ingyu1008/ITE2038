select Pokemon.name from Pokemon where Pokemon.id not in (select Evolution.before_id from Evolution) and Pokemon.type = 'Water' order by Pokemon.name;
