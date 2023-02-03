package com.notification.service.kafkaconsumer.model;

import java.util.Date;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonFormat;
@Entity
@Table(name="notification_portail")
public class Notification {
	@Id
	@GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "notif")
	@SequenceGenerator(name="notif", sequenceName = "SEQ_NOTIF", allocationSize=1)

	private Long id_notif;
@JsonFormat(pattern = "dd/MM/yyyy")
	private Date date_notif;
	private String libelle_notif;
	private String nom;
	private String type_notif; 

	private String cod_soc;
	private String mat_pers;
	private String id_sender;
	private String id_reciver;
	private Long counter;
	
	private String etat_notif;

	private String etat_notif_agent;

	

	public Notification(Date date_notif, String libelle_notif, String nom, String type_notif, String cod_soc,
			String mat_pers, String id_sender, String id_reciver, Long counter, String etat_notif,
			String etat_notif_agent) {
		super();
		this.date_notif = date_notif;
		this.libelle_notif = libelle_notif;
		this.nom = nom;
		this.type_notif = type_notif;
		this.cod_soc = cod_soc;
		this.mat_pers = mat_pers;
		this.id_sender = id_sender;
		this.id_reciver = id_reciver;
		this.counter = counter;
		this.etat_notif = etat_notif;
		this.etat_notif_agent = etat_notif_agent;
	}



	public Notification(Date date_notif, String libelle_notif, String nom, String type_notif, String cod_soc,
			String mat_pers, String id_sender, String id_reciver, Long counter) {
		super();
		this.date_notif = date_notif;
		this.libelle_notif = libelle_notif;
		this.nom = nom;
		this.type_notif = type_notif;
		this.cod_soc = cod_soc;
		this.mat_pers = mat_pers;
		this.id_sender = id_sender;
		this.id_reciver = id_reciver;
		this.counter = counter;
	}



	public Notification(Date date_notif, String libelle_notif, String nom, String type_notif, String cod_soc,
			String mat_pers, String id_sender, String id_reciver, Long counter, String etat_notif) {
		super();
		this.date_notif = date_notif;
		this.libelle_notif = libelle_notif;
		this.nom = nom;
		this.type_notif = type_notif;
		this.cod_soc = cod_soc;
		this.mat_pers = mat_pers;
		this.id_sender = id_sender;
		this.id_reciver = id_reciver;
		this.counter = counter;
		this.etat_notif = etat_notif;
	}



	public String getEtat_notif_agent() {
		return etat_notif_agent;
	}



	public void setEtat_notif_agent(String etat_notif_agent) {
		this.etat_notif_agent = etat_notif_agent;
	}



	public String getEtat_notif() {
		return etat_notif;
	}



	public void setEtat_notif(String etat_notif) {
		this.etat_notif = etat_notif;
	}



	public String getMat_pers() {
		return mat_pers;
	}



	public void setMat_pers(String mat_pers) {
		this.mat_pers = mat_pers;
	}



	public String getId_reciver() {
		return id_reciver;
	}



	public void setId_reciver(String id_reciver) {
		this.id_reciver = id_reciver;
	}



	public Notification(Long id_notif, Date date_notif, String libelle_notif, String nom, String type_notif,
			String cod_soc, String mat_pers, String id_sender, String id_reciver, Long counter) {
		super();
		this.id_notif = id_notif;
		this.date_notif = date_notif;
		this.libelle_notif = libelle_notif;
		this.nom = nom;
		this.type_notif = type_notif;
		this.cod_soc = cod_soc;
		this.mat_pers = mat_pers;
		this.id_sender = id_sender;
		this.id_reciver = id_reciver;
		this.counter = counter;
	}



	public String getCod_soc() {
		return cod_soc;
	}



	public void setCod_soc(String cod_soc) {
		this.cod_soc = cod_soc;
	}



	public String getMatpers() {
		return mat_pers;
	}



	public void setMatpers(String matpers) {
		this.mat_pers = matpers;
	}



	public String getId_sender() {
		return id_sender;
	}



	public void setId_sender(String id_sender) {
		this.id_sender = id_sender;
	}



	public String getId_receiver() {
		return id_reciver;
	}



	public void setId_receiver(String id_receiver) {
		this.id_reciver = id_receiver;
	}



	public Long getCounter() {
		return counter;
	}



	public void setCounter(Long counter) {
		this.counter = counter;
	}



	public Long getId_notif() {
		return id_notif;
	}

	public void setId_notif(Long id_notif) {
		this.id_notif = id_notif;
	}

	public Date getDate_notif() {
		return date_notif;
	}

	public void setDate_notif(Date date_notif) {
		this.date_notif = date_notif;
	}

	public String getLibelle_notif() {
		return libelle_notif;
	}

	public void setLibelle_notif(String libelle_notif) {
		this.libelle_notif = libelle_notif;
	}

	public String getNom() {
		return nom;
	}

	public void setNom(String nom) {
		this.nom = nom;
	}

	public String getType_notif() {
		return type_notif;
	}

	public void setType_notif(String type_notif) {
		this.type_notif = type_notif;
	}


	public Notification() {
		super();
	}



	@Override
	public String toString() {
		return "Notification [date_notif=" + date_notif + ", libelle_notif=" + libelle_notif + ", nom=" + nom
				+ ", type_notif=" + type_notif + ", cod_soc=" + cod_soc + ", mat_pers=" + mat_pers + ", id_sender="
				+ id_sender + ", id_reciver=" + id_reciver + ", counter=" + counter + "]";
	}
   
}
