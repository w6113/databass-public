## Test Data for A2

<table>
<thead>
    <td>File Name</td>
    <td>Description</td>
</thead>
<tbody>
<tr>
    <td><pre>test1.csv</pre></td>
    <td>All columns are randomly generated between (0, N) with uniform probability</td>
</tr>
<tr>
    <td><pre>test2.csv</pre></td>
    <td>
    <ul>
    <li>Column b is strongly correlated with test1's b</li>
    <li>All columns are randomly generated between (0, N) with uniform probability except for b</li>
    </ul>
    </td>
</tr>
<tr>
<td><pre>test3.csv</pre></td>
<td>
<ul>
<li>Column a and d is random uniform, but d is a string column</li>
<li>Column b is (0, 1) and c is (0, N) with uniform distribution</li>
</ul>
</td>
</tr>
<tr>
<td><pre>test4.csv</pre></td>
<td>
<ul>
<li>Column b is (0, 1) with skewed distribution</li>
<li>Column c is (0, N) with skewed distribution</li>
<li>Column d is a string with skewed distribution</li>
</ul>
</td>
</tr>
<tr>
<td><pre>test5.csv</pre></td>
<td>
<ul>
<li>Column b is (0, N) with normal distribution</li>
<li>Column c is (0, N) with left skewed distribution</li>
<li>Column d is (0, N) with right skewed distribution</li>
</ul>
</td>
</tr>
</tbody>
</table>