--
-- PostgreSQL database dump
--

SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: 
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


SET search_path = public, pg_catalog;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: tblcust; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tblcust (
    custnum character varying(255) NOT NULL,
    custconame character varying(255),
    custlname character varying(255),
    custfname character varying(255),
    custaddr1 character varying(255),
    custaddr2 character varying(255),
    custcity character varying(255),
    custstate character varying(255),
    custzip character varying(255),
    custphone character varying(255),
    custfax character varying(255),
    custemail character varying(255),
    custweb character varying(255),
    custnote text
);


ALTER TABLE public.tblcust OWNER TO postgres;

--
-- Name: tblinv; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tblinv (
    invnum integer NOT NULL,
    invdate timestamp without time zone,
    invrepnum character varying(255),
    invcustnum character varying(255),
    invnote text,
    invsubtotal character varying(25),
    invsalestax character varying(25),
    invshipping character varying(25),
    invtotal character varying(25),
    inviscomp boolean
);


ALTER TABLE public.tblinv OWNER TO postgres;

--
-- Name: tblinvdetail; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tblinvdetail (
    invdetailinvnum integer,
    invdetailprodnum character varying(255),
    invdetailproddescr character varying(255),
    invdetailprodprice character varying(25),
    invdetailqty integer
);


ALTER TABLE public.tblinvdetail OWNER TO postgres;

--
-- Name: tblprod; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tblprod (
    prodnum character varying(255) NOT NULL,
    proddescr character varying(255),
    prodprice character varying(25)
);


ALTER TABLE public.tblprod OWNER TO postgres;

--
-- Name: tblrep; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tblrep (
    repnum character varying(255) NOT NULL,
    replname character varying(255),
    repfname character varying(255)
);


ALTER TABLE public.tblrep OWNER TO postgres;

--
-- Name: tblstate; Type: TABLE; Schema: public; Owner: postgres; Tablespace: 
--

CREATE TABLE tblstate (
    stateabbr character varying(255) NOT NULL,
    statename character varying(255)
);


ALTER TABLE public.tblstate OWNER TO postgres;

--
-- Data for Name: tblcust; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tblcust (custnum, custconame, custlname, custfname, custaddr1, custaddr2, custcity, custstate, custzip, custphone, custfax, custemail, custweb, custnote) FROM stdin;
API	Atlantic Powers, Inc.	Jameson	Richard	3000 Powers Ct.	Bldg. 6	St. Peters	DE	00222-1212	111-111-1111	222-222-2222	api@api.com	www.api.com	Good client goes way back.\r\nOrders once a month.
BAC	Big Artichoke Company	Smith	James	6000 Stately Ln.	\N	St. Louis	MO	00002-1313	444-444-4444	444-444-4445	bac@bac.com	www.bac.com	OK client.
DCCB	Delta CCB, Inc.	Hendricks	Judy	2100 Delta Dr.	Suite 100	Joplin	MO	61123	333-333-3333	333-333-3334	judyh@deltaccb.com	www.deltaccb.com	Likes to buy the latest products.
HERR	Herrends, Inc.	Samualson	Steve	1200 Herrends Ind. Ct.	\N	St. Peters	MO	63124	555-555-5555	555-555-5556	\N	\N	\N
\.


--
-- Data for Name: tblinv; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tblinv (invnum, invdate, invrepnum, invcustnum, invnote, invsubtotal, invsalestax, invshipping, invtotal, inviscomp) FROM stdin;
1001	2009-01-01 00:00:00	Joe	API	\N	1474.9700	10.0000	40.0000	1524.9700	t
1002	2009-01-03 00:00:00	Mary	DCCB	\N	2041.1400	17.0000	20.0000	2078.1400	t
1003	2009-01-05 00:00:00	Joe	API	\N	1063.3000	18.0000	50.0000	1131.3000	t
1004	2009-01-07 00:00:00	Joe	BAC	\N	3689.8600	29.0000	60.0000	3778.8600	t
1005	2009-01-08 00:00:00	Mary	HERR	Expedite this order.	4615.8200	32.0000	30.0000	4677.8200	t
1006	2009-01-09 00:00:00	Joe	API	\N	31351.3800	34.0000	10.0000	31395.3800	f
1007	2009-01-12 00:00:00	Mary	HERR	\N	287.7000	36.0000	20.0000	343.7000	t
1008	2009-01-14 00:00:00	Joe	API	\N	701.4600	22.0000	30.0000	753.4600	t
1009	2009-01-22 00:00:00	Joe	BAC	\N	805.2400	24.0000	50.0000	879.2400	t
1010	2009-02-01 00:00:00	Joe	API	\N	435.5400	28.0000	60.0000	523.5400	t
1011	2009-02-06 00:00:00	Joe	API	\N	1119.4800	29.0000	40.0000	1188.4800	f
1012	2009-02-14 00:00:00	Joe	API	\N	543.4200	31.0000	10.0000	584.4200	t
1013	2009-02-17 00:00:00	Mary	DCCB	Expedite this order.	6597.3600	39.0000	60.0000	6696.3600	t
1014	2009-02-20 00:00:00	Mary	HERR	\N	1596.8400	40.0000	30.0000	1666.8400	t
1015	2009-02-20 00:00:00	Joe	BAC	\N	7665.2400	42.0000	20.0000	7727.2400	t
1016	2009-03-02 00:00:00	Joe	API	\N	212.6600	44.0000	50.0000	306.6600	t
1017	2009-03-09 00:00:00	Joe	API	\N	345.6600	60.0000	30.0000	435.6600	t
1018	2009-03-13 00:00:00	Mary	DCCB	\N	498.6800	69.0000	20.0000	587.6800	t
1019	2009-03-20 00:00:00	Joe	BAC	\N	601.5000	70.0000	10.0000	681.5000	t
1020	2009-03-30 00:00:00	Joe	API	\N	459.4200	12.0000	20.0000	491.4200	f
1021	2009-04-02 00:00:00	Mary	HERR	Expedite this order.	912.5600	19.0000	20.0000	951.5600	t
1022	2009-04-05 00:00:00	Joe	API	\N	595.2600	29.0000	30.0000	654.2600	t
1023	2009-04-05 00:00:00	Joe	BAC	\N	3949.6800	39.0000	40.0000	4028.6800	t
1024	2009-04-09 00:00:00	Joe	API	\N	367.6200	38.0000	10.0000	415.6200	t
1025	2009-04-12 00:00:00	Joe	API	\N	991.5600	36.0000	60.0000	1087.5600	t
1026	2009-04-18 00:00:00	Joe	BAC	\N	998.8200	46.0000	30.0000	1074.8200	t
1027	2009-04-18 00:00:00	Joe	API	\N	529.4400	85.0000	50.0000	664.4400	t
1028	2009-04-23 00:00:00	Mary	DCCB	Expedite this order.	583.3800	97.0000	20.0000	700.3800	t
1029	2009-04-29 00:00:00	Joe	BAC	\N	637.3200	16.0000	30.0000	683.3200	t
1030	2009-05-07 00:00:00	Joe	API	\N	1706.8400	37.0000	10.0000	1753.8400	t
1031	2009-05-09 00:00:00	Joe	BAC	\N	299.5800	69.0000	20.0000	388.5800	f
1032	2009-05-10 00:00:00	Joe	API	\N	709.1400	67.0000	30.0000	806.1400	t
1033	2009-05-17 00:00:00	Joe	BAC	\N	568.5800	49.0000	20.0000	637.5800	t
1034	2009-05-19 00:00:00	Mary	DCCB	\N	2141.4000	68.0000	30.0000	2239.4000	t
1035	2009-05-20 00:00:00	Mary	HERR	Expedite this order.	529.3200	58.0000	60.0000	647.3200	t
1036	2009-05-22 00:00:00	Joe	API	\N	927.2400	67.0000	40.0000	1034.2400	t
1037	2009-05-22 00:00:00	Joe	BAC	\N	1120.4200	49.0000	10.0000	1179.4200	t
1038	2009-05-22 00:00:00	Joe	API	\N	733.0800	34.0000	50.0000	817.0800	t
1039	2009-05-24 00:00:00	Joe	API	\N	1224.6400	43.0000	20.0000	1287.6400	t
1040	2009-05-30 00:00:00	Joe	BAC	\N	1342.4000	93.0000	30.0000	1465.4000	f
1041	2009-06-02 00:00:00	Joe	API	\N	936.8400	16.0000	50.0000	1002.8400	t
1042	2009-06-03 00:00:00	Mary	HERR	\N	1694.7600	76.0000	20.0000	1790.7600	t
1043	2009-06-03 00:00:00	Joe	BAC	\N	1528.7800	94.0000	30.0000	1652.7800	t
1044	2009-06-10 00:00:00	Joe	API	\N	721.2000	97.0000	10.0000	828.2000	t
1045	2009-06-15 00:00:00	Joe	API	\N	1645.1400	64.0000	60.0000	1769.1400	t
1046	2009-06-27 00:00:00	Joe	BAC	\N	292.6600	55.0000	30.0000	377.6600	t
1047	2009-07-07 00:00:00	Mary	DCCB	Expedite this order.	430.6200	44.0000	10.0000	484.6200	t
1048	2009-07-08 00:00:00	Joe	API	\N	448.1800	19.0000	50.0000	517.1800	t
1049	2009-07-09 00:00:00	Joe	BAC	\N	636.6400	39.0000	40.0000	715.6400	t
1050	2009-07-16 00:00:00	Joe	API	\N	743.4000	37.0000	60.0000	840.4000	t
1051	2009-07-17 00:00:00	Joe	API	\N	527.3400	68.0000	20.0000	615.3400	t
1052	2009-07-29 00:00:00	Mary	DCCB	\N	1050.5200	49.0000	30.0000	1129.5200	t
1053	2009-07-29 00:00:00	Joe	BAC	\N	1258.3800	69.0000	20.0000	1347.3800	t
1054	2009-08-01 00:00:00	Joe	API	\N	1241.1000	79.0000	20.0000	1340.1000	t
1055	2009-08-01 00:00:00	Joe	API	\N	1529.1600	49.0000	50.0000	1628.1600	t
1056	2009-08-02 00:00:00	Mary	HERR	Expedite this order.	5725.2000	39.0000	60.0000	5824.2000	f
1057	2009-08-06 00:00:00	Joe	BAC	\N	1624.8600	82.0000	40.0000	1746.8600	t
1058	2009-08-21 00:00:00	Joe	API	\N	1458.8800	65.0000	10.0000	1533.8800	t
1059	2009-09-02 00:00:00	Joe	BAC	\N	637.3200	91.0000	20.0000	748.3200	t
1060	2009-09-09 00:00:00	Mary	HERR	\N	7651.2600	76.0000	30.0000	7757.2600	t
1061	2009-09-19 00:00:00	Mary	DCCB	\N	222.7600	86.0000	40.0000	348.7600	t
1062	2009-09-23 00:00:00	Joe	API	\N	325.5600	96.0000	50.0000	471.5600	t
1063	2009-09-28 00:00:00	Mary	DCCB	\N	351.5400	74.0000	60.0000	485.5400	t
1064	2009-10-01 00:00:00	Mary	HERR	Expedite this order.	685.3800	65.0000	10.0000	760.3800	f
1065	2009-10-01 00:00:00	Joe	BAC	\N	725.6400	15.0000	20.0000	760.6400	t
1066	2009-10-03 00:00:00	Joe	API	\N	912.5600	35.0000	30.0000	977.5600	t
1067	2009-10-13 00:00:00	Joe	API	\N	561.3600	25.0000	40.0000	626.3600	t
1068	2009-10-29 00:00:00	Mary	DCCB	\N	1318.9000	52.0000	40.0000	1410.9000	t
1069	2009-11-05 00:00:00	Mary	HERR	\N	821.3600	63.0000	50.0000	934.3600	t
1070	2009-11-10 00:00:00	Joe	BAC	\N	575.3400	43.0000	60.0000	678.3400	f
1071	2009-11-10 00:00:00	Joe	API	\N	1203.3600	67.0000	50.0000	1320.3600	t
1072	2009-12-09 00:00:00	Joe	API	\N	8121.3000	94.0000	20.0000	8235.3000	t
1073	2009-12-09 00:00:00	Mary	DCCB	Expedite this order.	1431.2400	91.0000	30.0000	1552.2400	t
1074	2009-12-10 00:00:00	Joe	BAC	\N	1077.1800	129.0000	10.0000	1216.1800	t
1075	2009-12-10 00:00:00	Mary	HERR	\N	1874.6000	111.0000	30.0000	2015.6000	t
\.


--
-- Data for Name: tblinvdetail; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tblinvdetail (invdetailinvnum, invdetailprodnum, invdetailproddescr, invdetailprodprice, invdetailqty) FROM stdin;
1001	2-2345	Sledge Hammer	24.9900	2
1001	8-3245	Hack Saw	27.5000	40
1002	7-3453	Screw Driver	6.9900	6
1002	2-2345	Sledge Hammer	24.9900	80
1003	3-3432	Square Headed Hammer	22.4500	10
1003	7-3453	Screw Driver	6.9900	120
1004	7-3453	Screw Driver	6.9900	14
1004	3-3432	Square Headed Hammer	22.4500	160
1005	7-3453	Screw Driver	6.9900	18
1005	3-3432	Square Headed Hammer	22.4500	200
1006	7-3453	Screw Driver	6.9900	22
1006	1-3452	Silver Piping 1'	129.9900	240
1007	5-2342	Plyers	9.9900	26
1007	7-3453	Screw Driver	6.9900	4
1008	3-3432	Square Headed Hammer	22.4500	30
1008	7-3453	Screw Driver	6.9900	4
1009	3-3432	Square Headed Hammer	22.4500	34
1009	7-3453	Screw Driver	6.9900	6
1010	5-2342	Plyers	9.9900	38
1010	7-3453	Screw Driver	6.9900	8
1011	2-2345	Sledge Hammer	24.9900	42
1011	7-3453	Screw Driver	6.9900	10
1012	5-2342	Plyers	9.9900	46
1012	7-3453	Screw Driver	6.9900	12
1013	1-3452	Silver Piping 1'	129.9900	50
1013	7-3453	Screw Driver	6.9900	14
1014	8-3245	Hack Saw	27.5000	54
1014	7-3453	Screw Driver	6.9900	16
1015	1-3452	Silver Piping 1'	129.9900	58
1015	7-3453	Screw Driver	6.9900	18
1016	3-3432	Square Headed Hammer	22.4500	2
1016	7-3453	Screw Driver	6.9900	24
1017	2-2345	Sledge Hammer	24.9900	6
1017	7-3453	Screw Driver	6.9900	28
1018	8-3245	Hack Saw	27.5000	10
1018	7-3453	Screw Driver	6.9900	32
1019	2-2345	Sledge Hammer	24.9900	14
1019	7-3453	Screw Driver	6.9900	36
1020	5-2342	Plyers	9.9900	18
1020	7-3453	Screw Driver	6.9900	40
1021	8-3245	Hack Saw	27.5000	22
1021	7-3453	Screw Driver	6.9900	44
1022	5-2342	Plyers	9.9900	26
1022	7-3453	Screw Driver	6.9900	48
1023	1-3452	Silver Piping 1'	129.9900	30
1023	2-2345	Sledge Hammer	24.9900	2
1024	5-2342	Plyers	9.9900	34
1024	7-3453	Screw Driver	6.9900	4
1025	2-2345	Sledge Hammer	24.9900	38
1025	7-3453	Screw Driver	6.9900	6
1026	3-3432	Square Headed Hammer	22.4500	42
1026	7-3453	Screw Driver	6.9900	8
1027	5-2342	Plyers	9.9900	46
1027	7-3453	Screw Driver	6.9900	10
1028	5-2342	Plyers	9.9900	50
1028	7-3453	Screw Driver	6.9900	12
1029	5-2342	Plyers	9.9900	54
1029	7-3453	Screw Driver	6.9900	14
1030	8-3245	Hack Saw	27.5000	58
1030	7-3453	Screw Driver	6.9900	16
1031	5-2342	Plyers	9.9900	2
1031	7-3453	Screw Driver	6.9900	40
1032	2-2345	Sledge Hammer	24.9900	6
1032	7-3453	Screw Driver	6.9900	80
1033	8-3245	Hack Saw	27.5000	10
1033	7-3453	Screw Driver	6.9900	42
1034	1-3452	Silver Piping 1'	129.9900	14
1034	7-3453	Screw Driver	6.9900	46
1035	5-2342	Plyers	9.9900	18
1035	7-3453	Screw Driver	6.9900	50
1036	2-2345	Sledge Hammer	24.9900	22
1036	7-3453	Screw Driver	6.9900	54
1037	8-3245	Hack Saw	27.5000	26
1037	7-3453	Screw Driver	6.9900	58
1038	5-2342	Plyers	9.9900	30
1038	7-3453	Screw Driver	6.9900	62
1039	3-3432	Square Headed Hammer	22.4500	34
1039	7-3453	Screw Driver	6.9900	66
1040	3-3432	Square Headed Hammer	22.4500	38
1040	7-3453	Screw Driver	6.9900	70
1041	5-2342	Plyers	9.9900	42
1041	7-3453	Screw Driver	6.9900	74
1042	2-2345	Sledge Hammer	24.9900	46
1042	7-3453	Screw Driver	6.9900	78
1043	8-3245	Hack Saw	27.5000	50
1043	7-3453	Screw Driver	6.9900	22
1044	5-2342	Plyers	9.9900	54
1044	7-3453	Screw Driver	6.9900	26
1045	2-2345	Sledge Hammer	24.9900	58
1045	7-3453	Screw Driver	6.9900	28
1046	8-3245	Hack Saw	27.5000	2
1046	7-3453	Screw Driver	6.9900	34
1047	8-3245	Hack Saw	27.5000	6
1047	7-3453	Screw Driver	6.9900	38
1048	3-3432	Square Headed Hammer	22.4500	10
1048	7-3453	Screw Driver	6.9900	32
1049	8-3245	Hack Saw	27.5000	14
1049	7-3453	Screw Driver	6.9900	36
1050	2-2345	Sledge Hammer	24.9900	18
1050	7-3453	Screw Driver	6.9900	42
1051	5-2342	Plyers	9.9900	22
1051	7-3453	Screw Driver	6.9900	44
1052	8-3245	Hack Saw	27.5000	26
1052	7-3453	Screw Driver	6.9900	48
1053	8-3245	Hack Saw	27.5000	30
1053	7-3453	Screw Driver	6.9900	62
1054	2-2345	Sledge Hammer	24.9900	34
1054	7-3453	Screw Driver	6.9900	56
1055	5-2342	Plyers	9.9900	38
1055	2-2345	Sledge Hammer	24.9900	46
1056	1-3452	Silver Piping 1'	129.9900	42
1056	7-3453	Screw Driver	6.9900	38
1057	2-2345	Sledge Hammer	24.9900	46
1057	7-3453	Screw Driver	6.9900	68
1058	8-3245	Hack Saw	27.5000	50
1058	7-3453	Screw Driver	6.9900	12
1059	5-2342	Plyers	9.9900	54
1059	7-3453	Screw Driver	6.9900	14
1060	1-3452	Silver Piping 1'	129.9900	58
1060	7-3453	Screw Driver	6.9900	16
1061	8-3245	Hack Saw	27.5000	2
1061	7-3453	Screw Driver	6.9900	24
1062	5-2342	Plyers	9.9900	6
1062	7-3453	Screw Driver	6.9900	38
1063	5-2342	Plyers	9.9900	10
1063	7-3453	Screw Driver	6.9900	36
1064	2-2345	Sledge Hammer	24.9900	14
1064	7-3453	Screw Driver	6.9900	48
1065	3-3432	Square Headed Hammer	22.4500	18
1065	7-3453	Screw Driver	6.9900	46
1066	8-3245	Hack Saw	27.5000	22
1066	7-3453	Screw Driver	6.9900	44
1067	7-3453	Screw Driver	6.9900	26
1067	5-2342	Plyers	9.9900	38
1068	8-3245	Hack Saw	27.5000	30
1068	3-3432	Square Headed Hammer	22.4500	22
1069	7-3453	Screw Driver	6.9900	34
1069	3-3432	Square Headed Hammer	22.4500	26
1070	5-2342	Plyers	9.9900	38
1070	7-3453	Screw Driver	6.9900	28
1071	2-2345	Sledge Hammer	24.9900	42
1071	7-3453	Screw Driver	6.9900	22
1072	7-3453	Screw Driver	6.9900	46
1072	1-2452	Gold Plated Plyers	324.9900	24
1073	2-2345	Sledge Hammer	24.9900	50
1073	7-3453	Screw Driver	6.9900	26
1074	7-3453	Screw Driver	6.9900	54
1074	2-2345	Sledge Hammer	24.9900	28
1075	8-3245	Hack Saw	27.5000	58
1075	7-3453	Screw Driver	6.9900	40
1001	1-2452	Gold Plated Plyers	324.9900	1
\.


--
-- Data for Name: tblprod; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tblprod (prodnum, proddescr, prodprice) FROM stdin;
1-2452	Gold Plated Plyers	324.9900
1-3452	Silver Piping 1'	129.9900
2-2345	Sledge Hammer	24.9900
3-3432	Square Headed Hammer	22.4500
4-3242a	Sprocket Wrench	13.0000
5-2342	Plyers	9.9900
7-3453	Screw Driver	6.9900
8-3245	Hack Saw	27.5000
\.


--
-- Data for Name: tblrep; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tblrep (repnum, replname, repfname) FROM stdin;
Joe	Johnson	Joe
Mary	Smith	Mary
\.


--
-- Data for Name: tblstate; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY tblstate (stateabbr, statename) FROM stdin;
AE	Military Address
AK	Alaska
AL	Alabama
AP	Military Address
AR	Arkansas
AZ	Arizona
CA	California
CO	Colorado
CT	Connecticut
DC	District of Columbia
DE	Delaware
FL	Florida
GA	Georgia
GU	Guam
HI	Hawaii
IA	Iowa
ID	Idaho
IL	Illinois
IN	Indiana
KS	Kansas
KY	Kentucky
LA	Louisiana
MA	Massachusetts
MD	Maryland
ME	Maine
MI	Michigan
MN	Minnesota
MO	Missouri
MS	Mississippi
MT	Montana
NC	North Carolina
ND	North Dakota
NE	Nebraska
NH	New Hampshire
NJ	New Jersey
NM	New Mexico
NV	Nevada
NY	New York
OH	Ohio
OK	Oklahoma
OR	Oregon
PA	Pennsylvania
PR	Puerto Rico
RI	Rhode Island
SC	South Carolina
SD	South Dakota
TN	Tennessee
TX	Texas
UT	Utah
VA	Virginia
VI	Virgin Islands (US)
VT	Vermont
WA	Washington
WI	Wisconsin
WV	West Virginia
WY	Wyoming
\.


--
-- Name: tblcust_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY tblcust
    ADD CONSTRAINT tblcust_pkey PRIMARY KEY (custnum);


--
-- Name: tblinv_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY tblinv
    ADD CONSTRAINT tblinv_pkey PRIMARY KEY (invnum);


--
-- Name: tblprod_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY tblprod
    ADD CONSTRAINT tblprod_pkey PRIMARY KEY (prodnum);


--
-- Name: tblrep_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY tblrep
    ADD CONSTRAINT tblrep_pkey PRIMARY KEY (repnum);


--
-- Name: tblstate_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres; Tablespace: 
--

ALTER TABLE ONLY tblstate
    ADD CONSTRAINT tblstate_pkey PRIMARY KEY (stateabbr);


--
-- Name: tblinvdetail_invdetailinvnum_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY tblinvdetail
    ADD CONSTRAINT tblinvdetail_invdetailinvnum_fkey FOREIGN KEY (invdetailinvnum) REFERENCES tblinv(invnum);


--
-- Name: public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA public FROM postgres;
GRANT ALL ON SCHEMA public TO postgres;
GRANT ALL ON SCHEMA public TO PUBLIC;


--
-- PostgreSQL database dump complete
--

