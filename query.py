SELECT DISTINCT u.NAME AS Unit_Name, c.IDREC AS PressuresIDREC, me.IDRECPARENT AS GasIDREC, mo.name, u.idrec, mo.idrec
FROM ((unitsmetric.pvunit AS u INNER JOIN unitsmetric.pvunitcomp AS c ON c.IDRECPARENT = u.IDREC) INNER JOIN  unitsmetric.pvunitmeterorifice AS mo ON mo.IDRECPARENT = u.IDREC) INNER JOIN unitsmetric.pvunitmeterorificeentry AS me ON me.IDRECPARENT = mo.IDREC
WHERE mo.NAME LIKE '%Daily%' or mo.Name like '%Tester%'
   and (me.DELETED = 0 OR me.DELETED IS NULL) and mo.name IS not NULL
ORDER BY u.NAME, c.IDREC;