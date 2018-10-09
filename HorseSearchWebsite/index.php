<!DOCTYPE html>
<html>
<head>
	<title></title>
	<style type="text/css">
html, body {
    width: 100%;
    height: 100%;
    margin: 0;
    padding: 0;
    font-size: 17px;
    background-color: #444;
}

.wrapper {
    width: 90%;
    margin: auto;
    text-align: center;
    height: 5em;
    position: absolute;
    top: 0;
    bottom: 0;
    left: 0;
    right: 0;
}

.horselogo {
    font-weight: bold;
    font-size: 2em;
    color: #FFF;
}

input[name="q"] {
    font-size: 1.5em;
    border-radius: 0.2em;
    border: 1px solid #CCC;
    width: 100%;
    padding: 0.2em;
    box-sizing: border-box;
    text-align: center;
    outline: none;
    color: white;
    background-color: #555
}

input[type="button"], input[type="submit"] {
    font-size: 1em;
    margin-top: 0.5em;
    background-color: #ffbb5e;
    color: white;
    border: none;
    padding: 0.5em;
    border-radius: 0.2em;
}
	</style>
</head>
<body>
	<div class="wrapper">
		<div class="horselogo">HESTAMIN</div>
		<form method="POST" action="/search.php">
			<input type="text" name="q"><br>
			<input type="submit" name="search">
			<input type="button" name="lucky" value="I'm feeling lucky">
		</form>
	</div>
</body>
</html>
